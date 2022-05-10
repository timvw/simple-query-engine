use arrow2::datatypes::DataType;

#[derive(Debug, Clone)]
pub enum ScalarValue {
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    Utf8(Option<String>),
}

impl ScalarValue {
    pub fn data_type(&self) -> DataType {
        match self {
            ScalarValue::Int8(_) => DataType::Int8,
            ScalarValue::Int16(_) => DataType::Int16,
            ScalarValue::Int32(_) => DataType::Int32,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::Utf8(_) => DataType::Utf8,
        }
    }

    pub fn is_null(&self) -> bool {
        match self {
            ScalarValue::Int8(Some(_)) => false,
            ScalarValue::Int8(None) => true,
            ScalarValue::Int16(Some(_)) => false,
            ScalarValue::Int16(None) => true,
            ScalarValue::Int32(Some(_)) => false,
            ScalarValue::Int32(None) => true,
            ScalarValue::Int64(Some(_)) => false,
            ScalarValue::Int64(None) => true,
            ScalarValue::Utf8(Some(_)) => false,
            ScalarValue::Utf8(None) => true,
        }
    }

    pub fn utf8(value: &str) -> ScalarValue {
        ScalarValue::Utf8(Some(value.to_string()))
    }
    pub fn i32(value: i32) -> ScalarValue {
        ScalarValue::Int32(Some(value))
    }
}
