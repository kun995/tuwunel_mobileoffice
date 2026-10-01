use const_str::format as const_format;
use ruma::UserId;
use tuwunel_core::utils::html::escape as html_escape;
use tuwunel_service::oauth::server::format_user_code;

use super::DEVICE_HEAD;

pub(super) fn consent_html(
	user_id: &UserId,
	client_label: &str,
	user_code: &str,
	scope: &str,
	login_token: &str,
) -> String {
	// Token first, free-form client last: a later replace fills a placeholder left
	// in a request value.
	PAGE_HTML
		.replace("{token}", &html_escape(login_token))
		.replace("{code_value}", &html_escape(user_code))
		.replace("{code_display}", &html_escape(&format_user_code(user_code)))
		.replace("{user}", &html_escape(user_id.as_str()))
		.replace("{scope}", &html_escape(scope))
		.replace("{client}", &html_escape(client_label))
}

static PAGE_HTML: &str = const_format!(
	r#"
<!DOCTYPE html>
<html lang="en">
	<head>
		{DEVICE_HEAD}
		<title>Authorize device</title>
	</head>
	<body>
		<h1>Authorize device</h1>
		<p>A device is requesting to sign in as <strong>{{user}}</strong>.</p>
		<p>Application: <strong>{{client}}</strong></p>
		<p>Code: <code>{{code_display}}</code></p>
		<p>Requested access: <code>{{scope}}</code></p>
		<form method="POST" action="/_tuwunel/oidc/device_callback">
			<input type="hidden" name="user_code" value="{{code_value}}">
			<input type="hidden" name="loginToken" value="{{token}}">
			<button type="submit" name="action" value="approve" class="primary">
				Approve
			</button>
			<button type="submit" name="action" value="deny" class="danger">
				Deny
			</button>
		</form>
	</body>
</html>"#
);
