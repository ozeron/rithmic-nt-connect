//! Venue error conversion stays at the rithmic-rs boundary.
use rithmic_host::Error;

pub(crate) fn rithmic_error(value: rithmic_rs::RithmicError) -> Error {
    Error::Rithmic(value.to_string())
}
