use const_str::format as const_format;
use tuwunel_core::utils::html::escape as html_escape;

use super::DEVICE_HEAD;

pub(super) fn entry_html(error: Option<&str>) -> String {
	let error_block = error
		.map(|message| format!(r#"<p class="err">{}</p>"#, html_escape(message)))
		.unwrap_or_default();

	PAGE_HTML.replace("{error_block}", &error_block)
}

static PAGE_HTML: &str = const_format!(
	r#"
<!DOCTYPE html>
<html lang="en">
	<head>
		{DEVICE_HEAD}
		<title>Device sign-in</title>
	</head>
	<body>
		<h1>Device sign-in</h1>
		{{error_block}}
		<form method="GET" action="/_tuwunel/oidc/device">
			<label for="user_code">Enter the code shown on your device</label>
			<input id="user_code" name="user_code" autocomplete="off"
				autocapitalize="characters" spellcheck="false" required>
			<button type="submit" class="primary">Continue</button>
		</form>
	</body>
</html>"#
);
