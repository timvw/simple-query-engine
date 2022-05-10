use arrow2::datatypes::DataType;

#[derive(Debug, Clone)]
pub enum ScalarValue {
    Utf8(Option<String>),
}

impl ScalarValue {

    pub fn data_type(&self) -> DataType {
        match self {
            ScalarValue::Utf8(_) => DataType::Utf8,
        }
    }

    pub fn is_null(&self) -> bool {
        match self {
            ScalarValue::Utf8(Some(_)) => false,
            ScalarValue::Utf8(None) => true,
        }
    }

    pub fn utf8(value: &str) -> ScalarValue {
        ScalarValue::Utf8(Some(value.to_string()))
    }
}