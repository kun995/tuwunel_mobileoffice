use const_str::format as const_format;
use tuwunel_core::utils::html::escape as html_escape;

use super::DEVICE_HEAD;

pub(super) fn result_html(title: &str, message: &str) -> String {
	PAGE_HTML
		.replace("{title}", &html_escape(title))
		.replace("{message}", &html_escape(message))
}

static PAGE_HTML: &str = const_format!(
	r#"
<!DOCTYPE html>
<html lang="en">
	<head>
		{DEVICE_HEAD}
		<title>{{title}}</title>
	</head>
	<body>
		<h1>{{title}}</h1>
		<p>{{message}}</p>
	</body>
</html>"#
);
