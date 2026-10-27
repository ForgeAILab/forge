use db::task_condition::channels::ConditionMetadataValue;

pub(crate) fn set_condition(value: ConditionMetadataValue) -> db::TaskMetadataMutation {
    db::TaskMetadataMutation::Set {
        key: value.key().into(),
        value: serde_json::to_value(&value).expect("condition metadata serializes"),
    }
}
