use arrow::array::{ArrayRef, BooleanBuilder, Float64Builder, Int32Builder, ListBuilder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

pub fn json_to_arrow(json_values: &[Value]) -> Result<(Vec<ArrayRef>, Schema), Box<dyn std::error::Error>> {
  let mut builders: HashMap<String, Box<dyn std::any::Any>> = HashMap::new();
  let mut field_types: HashMap<String, DataType> = HashMap::new();

  // Inspect JSON structure to dynamically create fields and types
  for value in json_values.iter() {
    if let Some(obj) = value.as_object() {
      for (key, v) in obj.iter() {
        // Determine the type of the field dynamically
        match v {
          Value::String(_) => {
            field_types.entry(key.clone()).or_insert(DataType::Utf8);
            if !builders.contains_key(key) {
              builders.insert(key.clone(), Box::new(StringBuilder::new()));
            }
          }
          Value::Number(_) => {
            if v.is_f64() {
              field_types.entry(key.clone()).or_insert(DataType::Float64);
              if !builders.contains_key(key) {
                builders.insert(key.clone(), Box::new(Float64Builder::new()));
              }
            } else if v.is_i64() {
              field_types.entry(key.clone()).or_insert(DataType::Int32);
              if !builders.contains_key(key) {
                builders.insert(key.clone(), Box::new(Int32Builder::new()));
              }
            }
          }
          Value::Bool(_) => {
            field_types.entry(key.clone()).or_insert(DataType::Boolean);
            if !builders.contains_key(key) {
              builders.insert(key.clone(), Box::new(BooleanBuilder::new()));
            }
          }
          Value::Array(_) => {
            field_types
              .entry(key.clone())
              .or_insert(DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))));
            if !builders.contains_key(key) {
              builders.insert(key.clone(), Box::new(ListBuilder::new(StringBuilder::new())));
            }
          }
          _ => {}
        }
      }
    }
  }

  // Iterate over the json_values and build the fields
  for value in json_values.iter() {
    if let Some(obj) = value.as_object() {
      for (key, v) in obj.iter() {
        if let Some(builder) = builders.get_mut(key) {
          match builder.downcast_mut::<StringBuilder>() {
            Some(builder) => {
              if let Some(val) = v.as_str() {
                builder.append_value(val);
              } else {
                builder.append_null();
              }
            }
            None => match builder.downcast_mut::<Float64Builder>() {
              Some(builder) => {
                if let Some(val) = v.as_f64() {
                  builder.append_value(val);
                } else {
                  builder.append_null();
                }
              }
              None => match builder.downcast_mut::<Int32Builder>() {
                Some(builder) => {
                  if let Some(val) = v.as_i64() {
                    builder.append_value(val as i32);
                  } else {
                    builder.append_null();
                  }
                }
                None => match builder.downcast_mut::<BooleanBuilder>() {
                  Some(builder) => {
                    if let Some(val) = v.as_bool() {
                      builder.append_value(val);
                    } else {
                      builder.append_null();
                    }
                  }
                  None => match builder.downcast_mut::<ListBuilder<StringBuilder>>() {
                    Some(builder) => {
                      if let Some(array) = v.as_array() {
                        let inner_builder = builder.values(); // get the inner builder for the list
                        for item in array {
                          let str_val = item.as_str().unwrap_or_default();
                          inner_builder.append_value(str_val);
                        }
                        builder.append(true);
                      } else {
                        builder.append(false);
                      }
                    }
                    None => {}
                  },
                },
              },
            },
          }
        }
      }
    }
  }

  // Finish building the arrays for each field
  let mut arrays: Vec<ArrayRef> = Vec::new();
  let mut schema_fields: Vec<Field> = Vec::new();

  for (key, mut builder) in builders {
    if let Some(builder) = builder.downcast_mut::<StringBuilder>() {
      arrays.push(Arc::new(builder.finish_cloned()));
      schema_fields.push(Field::new(&key, DataType::Utf8, true));
    } else if let Some(builder) = builder.downcast_ref::<Float64Builder>() {
      arrays.push(Arc::new(builder.finish_cloned()));
      schema_fields.push(Field::new(&key, DataType::Float64, true));
    } else if let Some(builder) = builder.downcast_ref::<Int32Builder>() {
      arrays.push(Arc::new(builder.finish_cloned()));
      schema_fields.push(Field::new(&key, DataType::Int32, true));
    } else if let Some(builder) = builder.downcast_ref::<BooleanBuilder>() {
      arrays.push(Arc::new(builder.finish_cloned()));
      schema_fields.push(Field::new(&key, DataType::Boolean, true));
    } else if let Some(builder) = builder.downcast_ref::<ListBuilder<StringBuilder>>() {
      arrays.push(Arc::new(builder.finish_cloned()));
      schema_fields.push(Field::new(&key, DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), true));
    }
  }

  // Construct the schema
  let schema = Schema::new(schema_fields);

  Ok((arrays, schema))
}
