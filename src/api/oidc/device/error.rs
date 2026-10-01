use const_str::format as const_format;
use tuwunel_core::utils::html::escape as html_escape;

use super::DEVICE_HEAD;

pub(super) fn error_html(message: &str) -> String {
	PAGE_HTML.replace("{msg}", &html_escape(message))
}

static PAGE_HTML: &str = const_format!(
	r#"
<!DOCTYPE html>
<html lang="en">
	<head>
		{DEVICE_HEAD}
		<title>Error</title>
	</head>
	<body>
		<h1 class="err">Error</h1>
		<p>{{msg}}</p>
		<div class="nav">
			<a href="/_tuwunel/oidc/device">Try again</a>
		</div>
	</body>
</html>"#
);
