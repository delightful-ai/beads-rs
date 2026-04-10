use std::fs;
use std::io::Read;
use std::path::Path;

use crate::validation::{Result, validation_error};

#[derive(Clone, Copy)]
pub(crate) struct InlineTextArg<'a> {
    pub flag: &'a str,
    pub value: Option<&'a str>,
}

#[derive(Clone, Copy)]
pub(crate) struct FileTextArg<'a> {
    pub flag: &'a str,
    pub path: Option<&'a Path>,
}

pub(crate) fn text_input_uses_stdin(
    inline: &[InlineTextArg<'_>],
    files: &[FileTextArg<'_>],
    stdin_flag: bool,
    inline_dash_reads_stdin: bool,
) -> bool {
    stdin_flag
        || files
            .iter()
            .filter_map(|arg| arg.path)
            .any(|path| path == Path::new("-"))
        || (inline_dash_reads_stdin
            && inline
                .iter()
                .filter_map(|arg| arg.value)
                .any(|value| value == "-"))
}

pub(crate) fn resolve_text_input(
    field: &str,
    inline: &[InlineTextArg<'_>],
    files: &[FileTextArg<'_>],
    stdin_flag: bool,
    inline_dash_reads_stdin: bool,
) -> Result<Option<String>> {
    let mut stdin = std::io::stdin().lock();
    resolve_text_input_with_reader(
        field,
        inline,
        files,
        stdin_flag,
        inline_dash_reads_stdin,
        &mut stdin,
    )
}

fn resolve_text_input_with_reader<R: Read>(
    field: &str,
    inline: &[InlineTextArg<'_>],
    files: &[FileTextArg<'_>],
    stdin_flag: bool,
    inline_dash_reads_stdin: bool,
    stdin: &mut R,
) -> Result<Option<String>> {
    let mut sources = Vec::new();

    if stdin_flag {
        sources.push(TextSource::Stdin { flag: "--stdin" });
    }

    for arg in files {
        if let Some(path) = arg.path {
            if path == Path::new("-") {
                sources.push(TextSource::Stdin { flag: arg.flag });
            } else {
                sources.push(TextSource::File {
                    flag: arg.flag,
                    path,
                });
            }
        }
    }

    for arg in inline {
        if let Some(value) = arg.value {
            if inline_dash_reads_stdin && value == "-" {
                sources.push(TextSource::Stdin { flag: arg.flag });
            } else {
                sources.push(TextSource::Inline {
                    flag: arg.flag,
                    value,
                });
            }
        }
    }

    if sources.is_empty() {
        return Ok(None);
    }

    let mut resolved = Vec::with_capacity(sources.len());
    let mut stdin_value: Option<String> = None;

    for source in sources {
        let source_value = match source {
            TextSource::Inline { flag, value } => ResolvedTextSource {
                value: value.to_string(),
                display: format!("{flag}={value:?}"),
            },
            TextSource::File { flag, path } => {
                let content = fs::read_to_string(path).map_err(|err| {
                    validation_error(
                        field,
                        format!("failed to read {} {}: {err}", flag, path.display()),
                    )
                })?;
                ResolvedTextSource {
                    value: content,
                    display: format!("{flag}=@{}", path.display()),
                }
            }
            TextSource::Stdin { flag } => {
                let content = if let Some(existing) = stdin_value.as_ref() {
                    existing.clone()
                } else {
                    let mut content = String::new();
                    stdin.read_to_string(&mut content).map_err(|err| {
                        validation_error(field, format!("failed to read stdin for {flag}: {err}"))
                    })?;
                    stdin_value = Some(content.clone());
                    content
                };
                ResolvedTextSource {
                    value: content,
                    display: flag.to_string(),
                }
            }
        };
        resolved.push(source_value);
    }

    let first = resolved
        .first()
        .expect("resolved sources must be non-empty after non-empty input");
    for other in resolved.iter().skip(1) {
        if other.value != first.value {
            return Err(validation_error(
                field,
                format!(
                    "cannot specify both {} and {} with different values",
                    first.display, other.display
                ),
            ));
        }
    }

    Ok(Some(first.value.clone()))
}

enum TextSource<'a> {
    Inline { flag: &'a str, value: &'a str },
    File { flag: &'a str, path: &'a Path },
    Stdin { flag: &'a str },
}

struct ResolvedTextSource {
    value: String,
    display: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn resolve_text_input_accepts_matching_inline_aliases() {
        let value = resolve_text_input_with_reader(
            "description",
            &[
                InlineTextArg {
                    flag: "--description",
                    value: Some("same"),
                },
                InlineTextArg {
                    flag: "--message",
                    value: Some("same"),
                },
            ],
            &[],
            false,
            true,
            &mut Cursor::new(Vec::<u8>::new()),
        )
        .expect("matching aliases should succeed");

        assert_eq!(value.as_deref(), Some("same"));
    }

    #[test]
    fn resolve_text_input_rejects_conflicting_inline_aliases() {
        let err = resolve_text_input_with_reader(
            "description",
            &[
                InlineTextArg {
                    flag: "--description",
                    value: Some("a"),
                },
                InlineTextArg {
                    flag: "--message",
                    value: Some("b"),
                },
            ],
            &[],
            false,
            true,
            &mut Cursor::new(Vec::<u8>::new()),
        )
        .expect_err("conflicting aliases should fail");

        assert_eq!(
            err,
            validation_error(
                "description",
                "cannot specify both --description=\"a\" and --message=\"b\" with different values"
            )
        );
    }

    #[test]
    fn resolve_text_input_reads_body_file() {
        let dir = tempfile::tempdir().expect("temp dir");
        let body_path = dir.path().join("body.md");
        fs::write(&body_path, "from file\n").expect("write body");

        let value = resolve_text_input_with_reader(
            "description",
            &[],
            &[FileTextArg {
                flag: "--body-file",
                path: Some(body_path.as_path()),
            }],
            false,
            true,
            &mut Cursor::new(Vec::<u8>::new()),
        )
        .expect("body file should resolve");

        assert_eq!(value.as_deref(), Some("from file\n"));
    }

    #[test]
    fn resolve_text_input_rejects_conflicting_file_and_inline_values() {
        let dir = tempfile::tempdir().expect("temp dir");
        let body_path = dir.path().join("body.md");
        fs::write(&body_path, "from file\n").expect("write body");

        let err = resolve_text_input_with_reader(
            "description",
            &[InlineTextArg {
                flag: "--description",
                value: Some("inline"),
            }],
            &[FileTextArg {
                flag: "--body-file",
                path: Some(body_path.as_path()),
            }],
            false,
            true,
            &mut Cursor::new(Vec::<u8>::new()),
        )
        .expect_err("conflicting file and inline should fail");

        assert_eq!(
            err,
            validation_error(
                "description",
                format!(
                    "cannot specify both --body-file=@{} and --description=\"inline\" with different values",
                    body_path.display()
                )
            )
        );
    }

    #[test]
    fn resolve_text_input_reads_stdin_from_flag() {
        let value = resolve_text_input_with_reader(
            "description",
            &[],
            &[],
            true,
            true,
            &mut Cursor::new("from stdin\n"),
        )
        .expect("stdin should resolve");

        assert_eq!(value.as_deref(), Some("from stdin\n"));
    }

    #[test]
    fn resolve_text_input_reads_stdin_from_inline_dash() {
        let value = resolve_text_input_with_reader(
            "description",
            &[InlineTextArg {
                flag: "--message",
                value: Some("-"),
            }],
            &[],
            false,
            true,
            &mut Cursor::new("from stdin\n"),
        )
        .expect("stdin shorthand should resolve");

        assert_eq!(value.as_deref(), Some("from stdin\n"));
    }

    #[test]
    fn resolve_text_input_reads_design_file() {
        let dir = tempfile::tempdir().expect("temp dir");
        let design_path = dir.path().join("design.md");
        fs::write(&design_path, "design doc\n").expect("write design");

        let value = resolve_text_input_with_reader(
            "design",
            &[],
            &[FileTextArg {
                flag: "--design-file",
                path: Some(design_path.as_path()),
            }],
            false,
            false,
            &mut Cursor::new(Vec::<u8>::new()),
        )
        .expect("design file should resolve");

        assert_eq!(value.as_deref(), Some("design doc\n"));
    }

    #[test]
    fn text_input_uses_stdin_detects_dash_sources() {
        assert!(text_input_uses_stdin(
            &[InlineTextArg {
                flag: "--description",
                value: Some("-"),
            }],
            &[FileTextArg {
                flag: "--body-file",
                path: Some(Path::new("-")),
            }],
            false,
            true,
        ));

        assert!(!text_input_uses_stdin(
            &[InlineTextArg {
                flag: "--design",
                value: Some("-"),
            }],
            &[],
            false,
            false,
        ));
    }
}
