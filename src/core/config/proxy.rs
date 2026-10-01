//! Defines outbound proxy configuration and domain matching.
//!
//! The module supports no proxy, one global proxy, or domain-specific include
//! and exclude rules. URL matching selects the applicable proxy at request
//! time.

use std::{
	env::{var, var_os},
	fmt::Write as _,
	net::IpAddr,
	sync::Arc,
};

use http::Uri;
use ipnet::IpNet;
use reqwest::{ClientBuilder, NoProxy as ReqwestNoProxy, Proxy, Url};
use serde::Deserialize;
use smallstr::SmallString;
use smallvec::SmallVec;
use url::Host;

use crate::{Err, Result, implement, utils::url::hostname_matches_domain};

type Domain = SmallString<[u8; 32]>;
type Domains = Box<[Domain]>;
type Networks = Box<[Network]>;
type Proxies = SmallVec<[Proxy; 1]>;
type ProxyHostBuffer = SmallVec<[ProxyHost; 2]>;

/// Stores a proxy endpoint hostname inline when it is short.
///
/// Longer DNS names spill to the heap without changing resolver matching.
pub type ProxyHost = SmallString<[u8; 32]>;

/// Shared proxy endpoint names used by outbound resolver policy.
///
/// The slice is built once with the client proxy snapshot and shared by every
/// resolver that filters request destinations.
pub type ProxyHosts = Arc<[ProxyHost]>;
type ProxyUrlString = SmallString<[u8; 128]>;

#[derive(Clone, Copy, Eq, PartialEq)]
enum ProxyScheme {
	Http,
	Https,
	Socks4,
	Socks4a,
	Socks5,
	Socks5h,
}

/// Captures the proxy policy used by one generation of outbound clients.
///
/// The snapshot owns environment values and configured rules so resolver,
/// response checks, and lazily built clients retain one policy generation.
pub struct ProxySnapshot {
	configured: Option<Arc<ProxyConfig>>,
	environment: Option<EnvironmentProxy>,
	proxies: Proxies,
	hosts: ProxyHosts,
}

#[derive(Default)]
struct EnvironmentProxy {
	http: Option<ProxyScheme>,
	https: Option<ProxyScheme>,
	bypass: NoProxyRules,
}

#[derive(Clone, PartialEq)]
struct EnvironmentEndpoint {
	scheme: ProxyScheme,
	url: Url,
	host: Option<ProxyHost>,
}

#[derive(Default)]
struct EnvironmentProxyBuild {
	policy: EnvironmentProxy,
	proxies: [Option<Proxy>; 2],
	hosts: [Option<ProxyHost>; 2],
}

#[derive(Default)]
struct NoProxyRules {
	all_domains: bool,
	ips: Networks,
	domains: Domains,
}

enum Network {
	Address(IpAddr),
	Range(IpNet),
}

/// ## Examples:
/// - No proxy (default):
/// ```toml
/// proxy ="none"
/// ```
/// - Global proxy
/// ```toml
/// [global.proxy]
/// global = { url = "socks5h://localhost:9050" }
/// ```
/// - Proxy some domains
/// ```toml
/// [global.proxy]
/// [[global.proxy.by_domain]]
/// url = "socks5h://localhost:9050"
/// include = ["*.onion", "matrix.myspecial.onion"]
/// exclude = ["*.myspecial.onion"]
/// ```
/// ## Include vs. Exclude
/// If include is an empty list, it is assumed to be `["*"]`.
///
/// If a domain matches both the exclude and include list, the proxy will only
/// be used if it was included because of a more specific rule than it was
/// excluded. In the above example, the proxy would be used for
/// `ordinary.onion`, `matrix.myspecial.onion`, but not `hello.myspecial.onion`.
#[derive(Clone, Default, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProxyConfig {
	#[default]
	/// Adds no application-configured proxy.
	///
	/// The HTTP client builder remains unchanged, so automatic system proxy
	/// discovery may still apply. This is the default configuration.
	None,

	/// Routes every eligible request through one proxy.
	///
	/// The same proxy URL applies regardless of the request destination. The
	/// URL is parsed while the configuration is loaded.
	Global {
		/// Identifies the proxy endpoint.
		///
		/// The URL may select any proxy scheme supported by the HTTP client. It
		/// is converted into a request proxy during client construction.
		#[serde(deserialize_with = "crate::utils::deserialize_from_str")]
		url: Url,
	},

	/// Selects proxies using ordered domain rules.
	///
	/// Each rule may include or exclude wildcarded domains. The first rule that
	/// accepts a request supplies its proxy URL.
	ByDomain(Vec<PartialProxyConfig>),
}

/// Builds the HTTP client's proxy configuration.
///
/// The default configuration returns no application proxy. Global and
/// domain-based configuration produce the corresponding `reqwest` proxy
/// selector.
#[implement(ProxyConfig)]
pub fn to_proxy(&self) -> Result<Option<Proxy>> {
	self.validate_proxy_schemes()?;

	let proxy = match self {
		| Self::None => None,
		| Self::Global { url } => Some(Proxy::all(url.clone())?),
		| Self::ByDomain(_) => {
			let config = self.clone();

			Some(Proxy::custom(move |url| config.proxy_for(url).cloned()))
		},
	};

	Ok(proxy)
}

#[implement(ProxyConfig)]
fn validate_proxy_schemes(&self) -> Result {
	if let Some(url) = self
		.proxy_urls()
		.find(|url| ProxyScheme::parse(url.scheme()).is_none())
	{
		let scheme = url.scheme();

		return Err!(Config("proxy", "Unsupported proxy scheme: {scheme}"));
	}

	Ok(())
}

#[implement(ProxyScheme)]
fn parse(scheme: &str) -> Option<Self> {
	match scheme {
		| "http" => Some(Self::Http),
		| "https" => Some(Self::Https),
		| "socks4" => Some(Self::Socks4),
		| "socks4a" => Some(Self::Socks4a),
		| "socks5" => Some(Self::Socks5),
		| "socks5h" => Some(Self::Socks5h),
		| _ => None,
	}
}

#[implement(ProxyScheme)]
const fn as_str(self) -> &'static str {
	match self {
		| Self::Http => "http",
		| Self::Https => "https",
		| Self::Socks4 => "socks4",
		| Self::Socks4a => "socks4a",
		| Self::Socks5 => "socks5",
		| Self::Socks5h => "socks5h",
	}
}

/// Iterates over application-configured proxy endpoint names.
///
/// The iterator borrows the configured URLs and allocates no intermediate
/// collection. Automatic environment proxies are captured by `ProxySnapshot`.
#[implement(ProxyConfig)]
#[inline]
pub fn hosts(&self) -> impl Iterator<Item = &str> { self.proxy_urls().filter_map(proxy_hostname) }

fn proxy_hostname(url: &Url) -> Option<&str> {
	match url.host()? {
		| Host::Domain(host) => Some(host),
		| Host::Ipv4(_) | Host::Ipv6(_) => None,
	}
}

#[implement(ProxyConfig)]
fn proxy_urls(&self) -> impl Iterator<Item = &Url> {
	let global = match self {
		| Self::Global { url } => Some(url),
		| _ => None,
	};

	let domains = match self {
		| Self::ByDomain(proxies) => Some(proxies.as_slice()),
		| _ => None,
	};

	global.into_iter().chain(
		domains
			.into_iter()
			.flatten()
			.map(|proxy| &proxy.url),
	)
}

/// Reports whether an application-configured proxy carries a request URL.
///
/// Domain rules reuse the same include and exclude predicate as the reqwest
/// proxy selector.
#[implement(ProxyConfig)]
#[inline]
#[must_use]
pub fn intercepts(&self, url: &Url) -> bool { self.proxy_for(url).is_some() }

#[implement(ProxyConfig)]
fn proxy_for(&self, url: &Url) -> Option<&Url> {
	matches!(url.scheme(), "http" | "https")
		.then(|| match self {
			| Self::None => None,
			| Self::Global { url } => Some(url),
			| Self::ByDomain(proxies) => proxies
				.iter()
				.find_map(|proxy| proxy.for_url(url)),
		})
		.flatten()
		.filter(|proxy| ProxyScheme::parse(proxy.scheme()).is_some())
}

/// Captures the effective proxy configuration for new outbound clients.
///
/// Explicit configuration replaces reqwest's automatic environment proxy.
/// Otherwise the relevant environment variables are read once and normalized
/// to the effective endpoints used by reqwest's native matcher.
#[implement(ProxySnapshot)]
#[must_use]
pub fn new(config: &ProxyConfig) -> Result<Self> {
	Self::with_vars(config, var_os("REQUEST_METHOD").is_some(), |name| var(name).ok())
}

#[implement(ProxySnapshot)]
pub(super) fn with_vars<F>(config: &ProxyConfig, is_cgi: bool, var: F) -> Result<Self>
where
	F: Fn(&str) -> Option<String>,
{
	let configured = (!matches!(config, ProxyConfig::None)).then(|| Arc::new(config.clone()));
	let environment = matches!(config, ProxyConfig::None)
		.then(|| EnvironmentProxy::with_vars(is_cgi, &var))
		.transpose()?;

	let (environment, proxies, environment_hosts) = match environment {
		| Some(EnvironmentProxyBuild { policy, proxies, hosts }) =>
			(Some(policy), proxies.into_iter().flatten().collect(), hosts),
		| None => {
			let proxy = configured
				.as_ref()
				.map(ProxyConfig::to_proxy_shared)
				.transpose()?
				.flatten();

			(None, proxy.into_iter().collect(), [None, None])
		},
	};

	let hosts = config
		.hosts()
		.map(ProxyHost::from)
		.chain(environment_hosts.into_iter().flatten())
		.fold(ProxyHostBuffer::new(), |mut hosts, host| {
			if !hosts
				.iter()
				.any(|known| known.eq_ignore_ascii_case(&host))
			{
				hosts.push(host);
			}

			hosts
		});

	let spills = hosts.spilled() || hosts.iter().any(ProxyHost::spilled);
	let hosts = match hosts {
		| hosts if hosts.is_empty() => ProxyHosts::default(),
		| hosts if spills => ProxyHosts::from(hosts.into_vec()),
		| hosts => ProxyHosts::from(hosts.as_slice()),
	};

	Ok(Self { configured, environment, proxies, hosts })
}

#[implement(EnvironmentProxy)]
fn with_vars<F>(is_cgi: bool, var: &F) -> Result<EnvironmentProxyBuild>
where
	F: Fn(&str) -> Option<String>,
{
	if is_cgi {
		return Ok(EnvironmentProxyBuild::default());
	}

	let http = first_var(var, ["HTTP_PROXY", "http_proxy"])
		.as_deref()
		.and_then(parse_environment_url);

	let https = first_var(var, ["HTTPS_PROXY", "https_proxy"])
		.as_deref()
		.and_then(parse_environment_url);

	let all = (http.is_none() || https.is_none())
		.then(|| {
			first_var(var, ["ALL_PROXY", "all_proxy"])
				.as_deref()
				.and_then(parse_environment_url)
		})
		.flatten();

	if http.is_none() && https.is_none() && all.is_none() {
		return Ok(EnvironmentProxyBuild::default());
	}

	let no_proxy = first_var(var, ["NO_PROXY", "no_proxy"]).unwrap_or_default();
	let bypass = NoProxyRules::new(&no_proxy);
	let route = |proxy: &Option<EnvironmentEndpoint>| {
		proxy
			.as_ref()
			.or(all.as_ref())
			.map(|proxy| proxy.scheme)
	};

	let policy = Self {
		http: route(&http),
		https: route(&https),
		bypass,
	};

	let mut http = http;
	let mut https = https;
	let mut all = all;
	let take_host = |proxy: &mut Option<EnvironmentEndpoint>,
	                 all: &mut Option<EnvironmentEndpoint>| {
		proxy
			.as_mut()
			.and_then(|proxy| proxy.host.take())
			.or_else(|| all.as_mut().and_then(|proxy| proxy.host.take()))
	};

	let hosts = [take_host(&mut http, &mut all), take_host(&mut https, &mut all)];

	let proxies = environment_proxies(http, https, all, &no_proxy)?;

	Ok(EnvironmentProxyBuild { policy, proxies, hosts })
}

fn first_var<F>(var: &F, names: [&str; 2]) -> Option<String>
where
	F: Fn(&str) -> Option<String>,
{
	names.into_iter().find_map(var)
}

#[cfg(test)]
pub(super) fn parse_environment_proxy_url(raw: &str) -> Option<Url> {
	parse_environment_url(raw).map(|proxy| proxy.url)
}

fn parse_environment_url(raw: &str) -> Option<EnvironmentEndpoint> {
	let uri = raw.parse::<Uri>().ok()?;
	let scheme = uri
		.scheme_str()
		.map_or(Some(ProxyScheme::Http), ProxyScheme::parse)?;

	let authority = uri.authority()?;
	let (userinfo, host_port) = authority
		.as_str()
		.split_once('@')
		.map_or((None, authority.as_str()), |(userinfo, host_port)| (Some(userinfo), host_port));

	let uri = Uri::builder()
		.scheme(scheme.as_str())
		.authority(host_port)
		.path_and_query("/")
		.build()
		.ok()?;

	let host = proxy_uri_hostname(&uri).map(ProxyHost::from);
	let url = effective_environment_url(&uri, userinfo)?;

	Some(EnvironmentEndpoint { scheme, url, host })
}

fn proxy_uri_hostname(uri: &Uri) -> Option<&str> {
	let host = proxy_uri_host(uri.host()?);

	host.parse::<IpAddr>().is_err().then_some(host)
}

fn proxy_uri_host(host: &str) -> &str {
	host.strip_prefix('[')
		.and_then(|host| host.strip_suffix(']'))
		.unwrap_or(host)
}

fn effective_environment_url(uri: &Uri, userinfo: Option<&str>) -> Option<Url> {
	let scheme = uri.scheme_str()?;
	let authority = uri.authority()?;
	let host = proxy_uri_host(authority.host());

	let port = authority
		.port_u16()
		.unwrap_or(if scheme == "https" { 443 } else { 80 });

	let mut normalized = ProxyUrlString::new();

	normalized.push_str(scheme);
	normalized.push_str("://");
	if let Some(userinfo) = userinfo {
		normalized.push_str(userinfo);
		normalized.push('@');
	}

	if matches!(host.parse::<IpAddr>(), Ok(IpAddr::V6(_))) {
		normalized.push('[');
		normalized.push_str(host);
		normalized.push(']');
	} else {
		normalized.push_str(host);
	}

	write!(&mut normalized, ":{port}/").ok()?;
	Url::parse(&normalized).ok()
}

fn environment_proxies(
	http: Option<EnvironmentEndpoint>,
	https: Option<EnvironmentEndpoint>,
	all: Option<EnvironmentEndpoint>,
	no_proxy: &str,
) -> Result<[Option<Proxy>; 2]> {
	if http.is_none() && https.is_none() {
		let proxy = all
			.map(|proxy| {
				let bypass = ReqwestNoProxy::from_string(no_proxy);

				Proxy::all(proxy.url).map(|proxy| proxy.no_proxy(bypass))
			})
			.transpose()?;

		return Ok([proxy, None]);
	}

	let (http, https) = match (http, https, all) {
		| (Some(http), Some(https), _) => (Some(http), Some(https)),
		| (Some(http), None, all) => (Some(http), all),
		| (None, Some(https), all) => (all, Some(https)),
		| (None, None, _) => (None, None),
	};

	let bypass = ReqwestNoProxy::from_string(no_proxy);

	match (http, https) {
		| (None, None) => Ok([None, None]),
		| (Some(http), None) => {
			let proxy = Proxy::http(http.url)?.no_proxy(bypass);

			Ok([Some(proxy), None])
		},
		| (None, Some(https)) => {
			let proxy = Proxy::https(https.url)?.no_proxy(bypass);

			Ok([None, Some(proxy)])
		},
		| (Some(http), Some(https)) if http == https => {
			let proxy = Proxy::all(http.url)?.no_proxy(bypass);

			Ok([Some(proxy), None])
		},
		| (Some(http), Some(https)) => {
			let http = Proxy::http(http.url)?.no_proxy(bypass.clone());
			let https = Proxy::https(https.url)?.no_proxy(bypass);

			Ok([Some(http), Some(https)])
		},
	}
}

#[implement(ProxyConfig)]
fn to_proxy_shared(config: &Arc<Self>) -> Result<Option<Proxy>> {
	config.validate_proxy_schemes()?;

	let proxy = match config.as_ref() {
		| Self::None => None,
		| Self::Global { url } => Some(Proxy::all(url.clone())?),
		| Self::ByDomain(_) => {
			let config = Arc::clone(config);

			Some(Proxy::custom(move |url| config.proxy_for(url).cloned()))
		},
	};

	Ok(proxy)
}

/// Applies this snapshot to an outbound HTTP client builder.
///
/// Environment proxies are explicit so later environment changes cannot alter
/// a lazily built client.
#[implement(ProxySnapshot)]
#[must_use]
pub fn configure(&self, builder: ClientBuilder) -> ClientBuilder {
	let builder = if self.environment.is_some() {
		builder.no_proxy()
	} else {
		builder
	};

	self.proxies
		.iter()
		.cloned()
		.fold(builder, ClientBuilder::proxy)
}

/// Iterates over proxy endpoint names used by this snapshot.
///
/// The names include the configured surface or the effective environment
/// proxies, never both.
#[implement(ProxySnapshot)]
#[inline]
pub fn hosts(&self) -> impl Iterator<Item = &str> { self.hosts.iter().map(ProxyHost::as_str) }

/// Shares proxy endpoint names with a validating resolver.
///
/// Cloning the returned value increments one reference count and does not
/// duplicate any hostname.
#[implement(ProxySnapshot)]
#[inline]
#[must_use]
pub fn shared_hosts(&self) -> ProxyHosts { Arc::clone(&self.hosts) }

/// Reports whether this snapshot carries a request URL through a proxy.
///
/// Environment matching preserves scheme selection and `NO_PROXY`; explicit
/// rules use the configuration predicate.
#[implement(ProxySnapshot)]
#[inline]
#[must_use]
pub fn intercepts(&self, url: &Url) -> bool { self.proxy_scheme(url).is_some() }

/// Reports a destination that can alias a proxy resolver exemption.
///
/// Direct requests and local-DNS SOCKS requests resolve the destination in
/// this process, so guarded clients reject an endpoint-name collision.
#[implement(ProxySnapshot)]
#[must_use]
pub fn resolver_alias(&self, url: &Url) -> bool {
	let is_proxy_host = url.host_str().is_some_and(|host| {
		self.hosts
			.iter()
			.any(|proxy| proxy.eq_ignore_ascii_case(host))
	});

	is_proxy_host
		&& self
			.proxy_scheme(url)
			.is_none_or(|scheme| !scheme.resolves_remotely())
}

#[implement(ProxySnapshot)]
fn proxy_scheme(&self, url: &Url) -> Option<ProxyScheme> {
	self.configured
		.as_deref()
		.and_then(|proxy| proxy.proxy_for(url))
		.and_then(|proxy| ProxyScheme::parse(proxy.scheme()))
		.or_else(|| {
			self.environment
				.as_ref()
				.and_then(|proxy| proxy.proxy_for(url))
		})
}

#[implement(EnvironmentProxy)]
fn proxy_for(&self, url: &Url) -> Option<ProxyScheme> {
	let proxy = match url.scheme() {
		| "http" => self.http,
		| "https" => self.https,
		| _ => None,
	}?;

	let host = url.host()?;

	(!self.bypass.contains(&host)).then_some(proxy)
}

#[implement(ProxyScheme)]
const fn resolves_remotely(self) -> bool {
	matches!(self, Self::Http | Self::Https | Self::Socks4a | Self::Socks5h)
}

#[implement(NoProxyRules)]
fn new(raw: &str) -> Self {
	let (all_domains, ips, domains) = raw
		.split(',')
		.map(str::trim)
		.filter(|part| !part.is_empty())
		.fold(
			(false, Vec::new(), Vec::new()),
			|(mut all_domains, mut ips, mut domains), part| {
				if part == "*" {
					all_domains = true;
					domains.clear();
				} else if let Ok(network) = part.parse::<IpNet>() {
					ips.push(Network::Range(network));
				} else if let Ok(address) = part.parse::<IpAddr>() {
					ips.push(Network::Address(address));
				} else if !all_domains {
					domains.push(part.into());
				}

				(all_domains, ips, domains)
			},
		);

	Self {
		all_domains,
		ips: ips.into_boxed_slice(),
		domains: domains.into_boxed_slice(),
	}
}

#[implement(NoProxyRules)]
fn contains(&self, host: &Host<&str>) -> bool {
	match host {
		| Host::Ipv4(ip) => self.contains_ip(IpAddr::V4(*ip)),
		| Host::Ipv6(ip) => self.contains_ip(IpAddr::V6(*ip)),
		| Host::Domain(host) =>
			self.all_domains
				|| self
					.domains
					.iter()
					.any(|domain| hostname_matches_domain(host, domain)),
	}
}

#[implement(NoProxyRules)]
fn contains_ip(&self, ip: IpAddr) -> bool {
	self.ips.iter().any(|network| match network {
		| Network::Address(address) => *address == ip,
		| Network::Range(range) => range.contains(&ip),
	})
}

/// Associates one proxy URL with include and exclude patterns.
///
/// An empty include list matches every domain. When both lists match, the more
/// specific wildcard pattern decides whether the proxy applies.
#[derive(Clone, Debug, Deserialize)]
pub struct PartialProxyConfig {
	#[serde(deserialize_with = "crate::utils::deserialize_from_str")]
	url: Url,
	#[serde(default)]
	include: Vec<WildCardedDomain>,
	#[serde(default)]
	exclude: Vec<WildCardedDomain>,
}
impl PartialProxyConfig {
	#[must_use]
	/// Selects this rule's proxy URL for a request URL.
	///
	/// A URL without a domain does not match. Otherwise the most specific
	/// include and exclude patterns compete, with inclusion required for a
	/// result.
	pub fn for_url(&self, url: &Url) -> Option<&Url> {
		let domain = url.domain()?;
		let mut included_because = None; // most specific reason it was included
		let mut excluded_because = None; // most specific reason it was excluded
		if self.include.is_empty() {
			// treat empty include list as `*`
			included_because = Some(&WildCardedDomain::WildCard);
		}
		for wc_domain in &self.include {
			if wc_domain.matches(domain) {
				match included_because {
					| Some(prev) if !wc_domain.more_specific_than(prev) => (),
					| _ => included_because = Some(wc_domain),
				}
			}
		}
		for wc_domain in &self.exclude {
			if wc_domain.matches(domain) {
				match excluded_because {
					| Some(prev) if !wc_domain.more_specific_than(prev) => (),
					| _ => excluded_because = Some(wc_domain),
				}
			}
		}
		match (included_because, excluded_because) {
			| (Some(a), Some(b)) if a.more_specific_than(b) => Some(&self.url),
			| (Some(_), None) => Some(&self.url),
			| _ => None,
		}
	}
}

/// A domain name, that optionally allows a * as its first subdomain.
#[derive(Clone, Debug)]
enum WildCardedDomain {
	WildCard,
	WildCarded(String),
	Exact(String),
}
impl WildCardedDomain {
	fn matches(&self, domain: &str) -> bool {
		match self {
			| Self::WildCard => true,
			| Self::WildCarded(d) => domain.ends_with(d),
			| Self::Exact(d) => domain == d,
		}
	}

	fn more_specific_than(&self, other: &Self) -> bool {
		match (self, other) {
			| (Self::WildCard, Self::WildCard) => false,
			| (_, Self::WildCard) => true,
			| (Self::Exact(a), Self::WildCarded(_)) => other.matches(a),
			| (Self::WildCarded(a), Self::WildCarded(b)) => a != b && a.ends_with(b),
			| _ => false,
		}
	}
}
impl std::str::FromStr for WildCardedDomain {
	type Err = std::convert::Infallible;

	#[expect(clippy::string_slice)]
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		// maybe do some domain validation?
		Ok(if s.starts_with("*.") {
			Self::WildCarded(s[1..].to_owned())
		} else if s == "*" {
			Self::WildCarded(String::new())
		} else {
			Self::Exact(s.to_owned())
		})
	}
}
impl<'de> Deserialize<'de> for WildCardedDomain {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::de::Deserializer<'de>,
	{
		crate::utils::deserialize_from_str(deserializer)
	}
}
