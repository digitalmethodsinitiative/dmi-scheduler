-- jobs table
CREATE TABLE IF NOT EXISTS jobs (
  id                     SERIAL PRIMARY KEY,
  pythonfile             text,
  remote_id              text,
  details                text,
  timestamp              integer,
  timestamp_after        integer DEFAULT 0,
  timestamp_lastclaimed  integer DEFAULT 0,
  timestamp_claimed      integer DEFAULT 0,
  status                 text,
  attempts               integer DEFAULT 0,
  interval               integer DEFAULT 0
);

-- enforce
CREATE UNIQUE INDEX IF NOT EXISTS unique_job
  ON jobs (
    pythonfile,
    remote_id
  );
