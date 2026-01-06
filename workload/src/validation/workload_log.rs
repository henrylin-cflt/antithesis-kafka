use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, BufRead, BufReader},
    path::{Path, PathBuf},
};
use tracing::debug;
// use uuid::Uuid;

use crate::domain::{TestEvent, TestLogLine, ValidationFailure};

use super::log_validator::LogValidator;

#[derive(Debug, Serialize, Deserialize)]
struct WorkloadState {
    line_index: u64,
    validators: BTreeMap<String, String>,
}

pub struct WorkloadLog {
    workload_log_filename: PathBuf,
    workload_id: String,
    line_index: u64,
    log_validator: LogValidator,
}

impl WorkloadLog {
    pub fn new(
        workload_log_filename: impl AsRef<Path>,
        log_validator: LogValidator,
    ) -> Result<WorkloadLog> {
        let workload_id = workload_log_filename
            .as_ref()
            .file_stem()
            .and_then(|p| p.to_str())
            .ok_or_else(|| anyhow!("invalid workload log filename"))?
            .to_string();

        let mut workload_log = WorkloadLog {
            workload_log_filename: workload_log_filename.as_ref().to_path_buf(),
            workload_id,
            line_index: 1,
            log_validator,
        };
        workload_log.load_state()?;
        Ok(workload_log)
    }
    pub fn cursor(&self) -> u64 {
        self.line_index
    }
    fn load_state(&mut self) -> Result<()> {
        let workload_state_filename = self.workload_log_filename.with_extension("state");
        match std::fs::read_to_string(workload_state_filename) {
            Ok(state_data) => {
                let state: WorkloadState = serde_json::from_str(&state_data)
                    .context("failed to deserialize workload state from disk")?;
                self.line_index = state.line_index;
                self.log_validator
                    .load_state(state.validators)
                    .context("failed to load workload validation state from disk")?;
                Ok(())
            }
            Err(err) => {
                if err.kind() == io::ErrorKind::NotFound {
                    debug!("workload state not found on disk: {}", err);
                    Ok(())
                } else {
                    Err(anyhow!(
                        "failed to load workload state from disk: {:#?}",
                        err
                    ))
                }
            }
        }
    }
    fn save_state(&self) -> Result<()> {
        let workload_state_filename = self.workload_log_filename.with_extension("state");
        let state = WorkloadState {
            line_index: self.line_index,
            validators: self
                .log_validator
                .save_state()
                .context("failed to save workload validation state")?,
        };
        std::fs::write(
            workload_state_filename,
            serde_json::to_string(&state).context("failed to serialized workload state to disk")?,
        )
        .context("failed to write workload state to disk")?;
        Ok(())
    }
    pub fn validate(
        mut self,
    ) -> Result<(String, u64, BTreeMap<&'static str, Vec<ValidationFailure>>, bool)> {
        let workload_log_file =
            File::open(&self.workload_log_filename).context("failed to open workload log file")?;
        let mut lines = BufReader::new(workload_log_file)
            .lines()
            .skip((self.line_index - 1) as usize);
        let mut workload_ended = false;
        while let Some(Ok(line)) = lines.next() {
            let data = match serde_json::from_str(&line) {
                Ok(data) => data,
                Err(err) => {
                    debug!("Skipping invalid JSON at line {}: {}", self.line_index, err);
                    self.line_index += 1;
                    continue;
                }
            };
            
            let log = TestLogLine {
                line: self.line_index,
                data,
            };
            
            self.log_validator.validate_event(&log);
            if let TestEvent::WorkloadEnded = log.data.fields {
                workload_ended = true;
            }
            self.line_index += 1;
        }
        if !workload_ended {
            self.save_state()?;
        }
        Ok((
            self.workload_id,
            self.line_index,
            self.log_validator.finalize(),
            workload_ended,
        ))
    }
}
