-- create a table
CREATE TABLE IF NOT EXISTS students (
  id numeric ,
  name text NOT NULL,
  gender text NOT NULL,
  PRIMARY KEY(id)
);
-- insert some values
INSERT INTO students VALUES (1, 'Ryan', 'M');
INSERT INTO students VALUES (2, 'Joanna', 'F');