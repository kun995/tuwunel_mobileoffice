use figment::value::{Dict, Value};
use smallstr::SmallString;
use smallvec::SmallVec;
use toml_writer::TomlWrite as _;

use super::SectionSpec;
use crate::implement;

pub(super) type ConfigPath<'a> = SmallVec<[PathPart<'a>; 4]>;
type SchemaPath<'a> = SmallVec<[&'a str; 4]>;
type SectionPath<'a> = SmallVec<[SectionPart<'a>; 4]>;
type Instances<'a> = SmallVec<[Instance<'a>; 1]>;
type ResolvedSection = SmallString<[u8; 48]>;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum PathPart<'a> {
	Key(&'a str),
	Index(usize),
}

#[derive(Clone, Copy)]
enum SectionPart<'a> {
	Literal(&'a str),
	Dynamic(&'a str),
}

#[derive(Clone, Copy)]
enum SectionShape {
	Table,
	Array,
}

#[derive(Clone, Copy)]
enum Node<'a> {
	Root(&'a Dict),
	Value(&'a Value),
}

pub(super) struct Instance<'a> {
	pub(super) section: ResolvedSection,
	pub(super) path: ConfigPath<'a>,
	pub(super) values: &'a Dict,
}

pub(super) fn resolve_instances<'a>(spec: &SectionSpec, values: &'a Dict) -> Instances<'a> {
	let shape = if spec.section.starts_with('[') {
		SectionShape::Array
	} else {
		SectionShape::Table
	};

	let section = spec.section.trim_matches(['[', ']']);
	let parts = section.split('.').collect::<SchemaPath<'_>>();
	let mut path = ConfigPath::new();
	let mut section_path = SectionPath::new();
	let mut instances = Instances::new();

	resolve_parts(
		Node::Root(values),
		&parts,
		shape,
		&mut path,
		&mut section_path,
		&mut instances,
	);

	instances
}

fn resolve_parts<'a>(
	node: Node<'a>,
	parts: &[&'a str],
	shape: SectionShape,
	path: &mut ConfigPath<'a>,
	section: &mut SectionPath<'a>,
	instances: &mut Instances<'a>,
) {
	let Some((part, tail)) = parts.split_first() else {
		resolve_leaf(node, shape, path, section, instances);
		return;
	};

	if *part == "global" && path.is_empty() {
		section.push(SectionPart::Literal(part));
		resolve_parts(node, tail, shape, path, section, instances);
		section.pop();

		return;
	}

	if let Some(choices) = part
		.strip_prefix('<')
		.and_then(|part| part.strip_suffix('>'))
	{
		let Some(values) = node.as_dict() else {
			return;
		};

		if choices.contains('|') {
			for choice in choices.split('|') {
				let Some(value) = values.get(choice) else {
					continue;
				};

				path.push(PathPart::Key(choice));
				section.push(SectionPart::Literal(choice));
				resolve_parts(Node::Value(value), tail, shape, path, section, instances);
				section.pop();
				path.pop();
			}
		} else {
			for (key, value) in values {
				path.push(PathPart::Key(key));
				section.push(SectionPart::Dynamic(key));
				resolve_parts(Node::Value(value), tail, shape, path, section, instances);
				section.pop();
				path.pop();
			}
		}

		return;
	}

	let Some(value) = node
		.as_dict()
		.and_then(|values| values.get(*part))
	else {
		return;
	};

	path.push(PathPart::Key(part));
	section.push(SectionPart::Literal(part));
	resolve_parts(Node::Value(value), tail, shape, path, section, instances);
	section.pop();
	path.pop();
}

fn resolve_leaf<'a>(
	node: Node<'a>,
	shape: SectionShape,
	path: &mut ConfigPath<'a>,
	section: &SectionPath<'a>,
	instances: &mut Instances<'a>,
) {
	match shape {
		| SectionShape::Table =>
			if let Some(values) = node.as_dict() {
				instances.push(Instance {
					section: render_section_path(section),
					path: path.clone(),
					values,
				});
			},
		| SectionShape::Array => {
			let Some(values) = node.as_array() else {
				return;
			};

			for (index, value) in values.iter().enumerate() {
				let Some(values) = value.as_dict() else {
					continue;
				};

				path.push(PathPart::Index(index));
				instances.push(Instance {
					section: render_section_path(section),
					path: path.clone(),
					values,
				});

				path.pop();
			}
		},
	}
}

#[implement(Node, generics = "<'a>", params = "<'a>")]
fn as_dict(self) -> Option<&'a Dict> {
	match self {
		| Self::Root(values) => Some(values),
		| Self::Value(value) => value.as_dict(),
	}
}

#[implement(Node, generics = "<'a>", params = "<'a>")]
fn as_array(self) -> Option<&'a [Value]> {
	match self {
		| Self::Root(_) => None,
		| Self::Value(value) => value.as_array(),
	}
}

pub(super) fn normalize_aliases(values: &mut Dict, schema: &[&SectionSpec]) {
	for &spec in schema {
		let shape = if spec.section.starts_with('[') {
			SectionShape::Array
		} else {
			SectionShape::Table
		};

		let section = spec.section.trim_matches(['[', ']']);
		let parts = section.split('.').collect::<SchemaPath<'_>>();
		let parts = parts.strip_prefix(&["global"]).unwrap_or(&parts);

		normalize_dict(values, parts, shape, spec);
	}
}

fn normalize_dict(values: &mut Dict, parts: &[&str], shape: SectionShape, spec: &SectionSpec) {
	let Some((part, tail)) = parts.split_first() else {
		if matches!(shape, SectionShape::Table) {
			normalize_fields(values, spec);
		}

		return;
	};

	if let Some(choices) = part
		.strip_prefix('<')
		.and_then(|part| part.strip_suffix('>'))
	{
		if choices.contains('|') {
			for choice in choices.split('|') {
				if let Some(value) = values.get_mut(choice) {
					normalize_value(value, tail, shape, spec);
				}
			}
		} else {
			for value in values.values_mut() {
				normalize_value(value, tail, shape, spec);
			}
		}

		return;
	}

	if tail.is_empty() {
		rename_alias(values, part, spec.aliases);
	}

	if let Some(value) = values.get_mut(*part) {
		normalize_value(value, tail, shape, spec);
	}
}

fn normalize_value(value: &mut Value, parts: &[&str], shape: SectionShape, spec: &SectionSpec) {
	if parts.is_empty() {
		match (shape, value) {
			| (SectionShape::Table, Value::Dict(_, values)) => normalize_fields(values, spec),
			| (SectionShape::Array, Value::Array(_, values)) => values
				.iter_mut()
				.filter_map(|value| match value {
					| Value::Dict(_, values) => Some(values),
					| _ => None,
				})
				.for_each(|values| normalize_fields(values, spec)),
			| _ => {},
		}

		return;
	}

	if let Value::Dict(_, values) = value {
		normalize_dict(values, parts, shape, spec);
	}
}

fn normalize_fields(values: &mut Dict, spec: &SectionSpec) {
	for field in spec.fields {
		rename_alias(values, field.name, field.aliases);
	}
}

fn rename_alias(values: &mut Dict, canonical: &str, aliases: &[&str]) {
	if values.contains_key(canonical) {
		return;
	}

	let Some(alias) = aliases
		.iter()
		.find(|alias| values.contains_key(**alias))
		.copied()
	else {
		return;
	};

	let Some(value) = values.remove(alias) else {
		return;
	};

	values.insert(canonical.to_owned(), value);
}

fn render_section_path(parts: &[SectionPart<'_>]) -> ResolvedSection {
	let mut section = ResolvedSection::new();

	for (index, part) in parts.iter().enumerate() {
		if index > 0 {
			section
				.key_sep()
				.expect("written to section buffer");
		}

		match part {
			| SectionPart::Literal(part) => section.push_str(part),
			| SectionPart::Dynamic(part) => section
				.key(*part)
				.expect("written to section buffer"),
		}
	}

	section
}

pub(super) fn find_path<'a>(values: &'a Dict, path: &[PathPart<'_>]) -> Option<&'a Value> {
	let (head, tail) = path.split_first()?;
	let PathPart::Key(key) = head else {
		return None;
	};

	let value = values.get(*key)?;

	find_value(value, tail)
}

fn find_value<'a>(value: &'a Value, path: &[PathPart<'_>]) -> Option<&'a Value> {
	let Some((head, tail)) = path.split_first() else {
		return Some(value);
	};

	let value = match head {
		| PathPart::Key(key) => value.as_dict()?.get(*key)?,
		| PathPart::Index(index) => value.as_array()?.get(*index)?,
	};

	find_value(value, tail)
}

pub(super) fn remove_path(values: &mut Dict, path: &[PathPart<'_>]) {
	let Some((head, tail)) = path.split_first() else {
		return;
	};

	let PathPart::Key(key) = head else {
		return;
	};

	if tail.is_empty() {
		values.remove(*key);
		return;
	}

	if let Some(value) = values.get_mut(*key) {
		remove_value_path(value, tail);
	}
}

fn remove_value_path(value: &mut Value, path: &[PathPart<'_>]) {
	let Some((head, tail)) = path.split_first() else {
		return;
	};

	match head {
		| PathPart::Index(index) => {
			let Value::Array(_, values) = value else {
				return;
			};

			if let Some(value) = values.get_mut(*index) {
				remove_value_path(value, tail);
			}
		},
		| PathPart::Key(key) => {
			let Value::Dict(_, values) = value else {
				return;
			};

			if tail.is_empty() {
				values.remove(*key);
				return;
			}

			let Some(value) = values.get_mut(*key) else {
				return;
			};

			remove_value_path(value, tail);
		},
	}
}
