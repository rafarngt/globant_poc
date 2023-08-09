CREATE OR REPLACE TABLE your_dataset.hired_employees (
    id INT64, -- Id of the employee
    name STRING, -- Name and surname of the employee
    hire_datetime STRING, -- Hire datetime in ISO format
    department_id INT64, -- Id of the department which the employee was hired for
    job_id INT64 -- Id of the job which the employee was hired for
);
