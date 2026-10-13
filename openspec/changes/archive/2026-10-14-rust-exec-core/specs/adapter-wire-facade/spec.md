## ADDED Requirements

### Requirement: Execution rules come from the extension
The Python adapter SHALL obtain the lifecycle policy, exposure derivation, commission lookup, notification coercion and fill dedup from the extension and SHALL keep only Nautilus translation and event-loop wiring. Module and class names that the exec and data clients import SHALL remain importable from their current Python paths during migration.

#### Scenario: Existing imports keep working
- **WHEN** code imports `OrderPlantPolicy` or `OrderPlantState` from `rithmic_nt_connect._order_plant`
- **THEN** the import SHALL succeed and the object SHALL behave as before

#### Scenario: No duplicate rule in Python
- **WHEN** a ported rule is searched for in the Python package
- **THEN** only the binding or re-export SHALL be found, not a second implementation
