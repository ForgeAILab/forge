//! Parsing shared by local and daemon outbox harvesters.
use serde_json::Value;

/// Parse JSONL, pretty-printed, or concatenated objects with stable receipt positions.
pub fn parse_entries(text: &str, limit: usize) -> Vec<(String, Result<Value, String>)> {
    let line_starts = std::iter::once(0)
        .chain(text.match_indices('\n').map(|(offset, _)| offset + 1))
        .collect::<Vec<_>>();
    let mut entries = Vec::new();
    let mut offset = 0;
    let mut previous_line = 0;
    while offset < text.len() {
        offset += text.as_bytes()[offset..]
            .iter()
            .take_while(|byte| matches!(**byte, b' ' | b'\t' | b'\r' | b'\n'))
            .count();
        if offset == text.len() {
            break;
        }
        if entries.len() == limit {
            entries.push((
                "limit".to_owned(),
                Err(format!("only the first {limit} entries were ingested")),
            ));
            break;
        }
        let line_no = line_starts.partition_point(|start| *start <= offset);
        // Preserve existing JSONL receipt keys; a second object on the same
        // physical line also needs its byte column to remain distinct.
        let position = if line_no == previous_line {
            format!("{line_no}:{}", offset - line_starts[line_no - 1] + 1)
        } else {
            line_no.to_string()
        };
        previous_line = line_no;
        let mut stream = serde_json::Deserializer::from_str(&text[offset..]).into_iter::<Value>();
        let Some(entry) = stream.next() else {
            break;
        };
        match entry {
            Ok(value) => {
                offset += stream.byte_offset();
                entries.push((
                    position,
                    if value.is_object() {
                        Ok(value)
                    } else {
                        Err("entry must be a JSON object".to_owned())
                    },
                ));
            }
            Err(error) => {
                entries.push((position, Err(error.to_string())));
                offset += text[offset..]
                    .find('\n')
                    .map_or(text.len() - offset, |index| index + 1);
            }
        }
    }
    entries
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn outbox_parses_pretty_concatenated_objects_and_resumes_after_bad_lines() {
        let entries = parse_entries("{\n  \"kind\": \"progress\"\n} {\"kind\":\"validation\"}\ninvalid\n{\"kind\":\"decision\"}", 200);
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0].0, "1");
        assert_eq!(entries[1].0, "3");
        assert!(entries[2].1.is_err());
        assert_eq!(entries[3].1.as_ref().unwrap()["kind"], "decision");
        let same_line = parse_entries("{}{}", 200);
        assert_eq!(same_line[0].0, "1");
        assert_eq!(same_line[1].0, "1:3");
    }
}
