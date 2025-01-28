--creating warehouses, databases, and schemas
CREATE WAREHOUSE Patient_Management_System;
USE WAREHOUSE Patient_Management_System;

CREATE DATABASE Patient_Database;
USE DATABASE Patient_Database;

CREATE SCHEMA Patient_Database.Call_History;
CREATE SCHEMA Patient_Database.Feedback_History;
CREATE SCHEMA Patient_Database.Patient_Health_Records


--creating storage integration for GCP buckets
CREATE STORAGE INTEGRATION Patient_Data
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'GCS'
ENABLED = TRUE
STORAGE_ALLOWED_LOCATIONS = ('gcs://output_call_history/', 'gcs://output_feedback_history/', 'gcs://patient_management_system/', 'gcs://output_patient_data/');

DESC STORAGE INTEGRATION Patient_Data;


--creating notification integrations for the pub/sub subscriptions
CREATE NOTIFICATION INTEGRATION call_notification_integration
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = GCP_PUBSUB
  ENABLED = true
  GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/robust-index-446813-f4/subscriptions/call-subscription';

CREATE NOTIFICATION INTEGRATION feedback_notification_integration
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = GCP_PUBSUB
  ENABLED = true
  GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/robust-index-446813-f4/subscriptions/feedback-subscription';

CREATE NOTIFICATION INTEGRATION patient_data_integration
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = GCP_PUBSUB
  ENABLED = true
  GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/robust-index-446813-f4/subscriptions/patient-data-subscription';

DESC NOTIFICATION INTEGRATION call_notification_integration;
DESC NOTIFICATION INTEGRATION feedback_notification_integration;
DESC NOTIFICATION INTEGRATION patient_data_integration;


--creating role PMS_Admin and granting all privileges
CREATE ROLE PMS_Admin;
GRANT CREATE STAGE ON SCHEMA Patient_Database.Call_History TO ROLE PMS_Admin;
GRANT CREATE STAGE ON SCHEMA Patient_Database.Feedback_History TO ROLE PMS_Admin;
GRANT CREATE STAGE ON SCHEMA Patient_Database.Patient_Health_Records TO ROLE PMS_Admin;
GRANT USAGE ON DATABASE Patient_Database TO ROLE PMS_Admin;
GRANT USAGE ON SCHEMA Patient_Database.Call_History TO ROLE PMS_Admin;
GRANT USAGE ON SCHEMA Patient_Database.Feedback_History TO ROLE PMS_Admin;
GRANT USAGE ON SCHEMA Patient_Database.Patient_Health_Records TO ROLE PMS_Admin;
GRANT USAGE ON INTEGRATION Patient_Data TO ROLE PMS_Admin;


--creating external stages for call history, feedback history and patient health data
CREATE STAGE Call_History.call_history_stage
URL = 'gcs://output_call_history/'
STORAGE_INTEGRATION = Patient_Data
FILE_FORMAT = (TYPE = 'PARQUET');

CREATE STAGE Feedback_History.feedback_history_stage
URL = 'gcs://output_feedback_history/'
STORAGE_INTEGRATION = Patient_Data
FILE_FORMAT = (TYPE = 'PARQUET');

CREATE STAGE Patient_Health_Records.patient_data_stage
URL = 'gcs://patient_management_system'
STORAGE_INTEGRATION = Patient_Data
FILE_FORMAT = (TYPE = 'JSON');

CREATE STAGE Patient_Health_Records.appointment_stage
URL = 'gcs://patient_management_system/Appointment/'
STORAGE_INTEGRATION = Patient_Data
FILE_FORMAT = (TYPE = 'JSON');

CREATE STAGE Patient_Health_Records.condition_stage
URL = 'gcs://patient_management_system/Condition/'
STORAGE_INTEGRATION = Patient_Data
FILE_FORMAT = (TYPE = 'JSON');

CREATE STAGE Patient_Health_Records.endpoint_stage
URL = 'gcs://patient_management_system/Endpoint/'
STORAGE_INTEGRATION = Patient_Data
FILE_FORMAT = (TYPE = 'JSON');

CREATE STAGE Patient_Health_Records.organization_stage
URL = 'gcs://patient_management_system/Organization/'
STORAGE_INTEGRATION = Patient_Data
FILE_FORMAT = (TYPE = 'JSON');

CREATE STAGE Patient_Health_Records.patient_stage
URL = 'gcs://patient_management_system/Patient/'
STORAGE_INTEGRATION = Patient_Data
FILE_FORMAT = (TYPE = 'JSON');

CREATE STAGE Patient_Health_Records.practitioner_stage
URL = 'gcs://patient_management_system/Practitioner/'
STORAGE_INTEGRATION = Patient_Data
FILE_FORMAT = (TYPE = 'JSON');

DESC STAGE Call_History.call_history_stage;
DESC STAGE Feedback_History.feedback_history_stage;
DESC STAGE Patient_Health_Records.patient_data_stage
DESC STAGE Patient_Health_Records.appointment_stage
DESC STAGE Patient_Health_Records.condition_stage
DESC STAGE Patient_Health_Records.endpoint_stage
DESC STAGE Patient_Health_Records.organization_stage
DESC STAGE Patient_Health_Records.patient_stage
DESC STAGE Patient_Health_Records.practitioner_stage


--creating tables to store data
CREATE TABLE Patient_Database.Call_History.patient_call_records (
    call_id VARCHAR,
    patient_id VARCHAR,
    timestamp DATETIME,
    call_type VARCHAR,
    resolution_status VARCHAR,
    department VARCHAR,
    call_duration INT,
    notes TEXT,
    follow_up_required VARCHAR,
    satisfaction_score INT,
    call_centre VARCHAR
);

CREATE TABLE Patient_Database.Feedback_History.patient_feedback_records (
    feedback_id VARCHAR,
    patient_id VARCHAR,
    timestamp DATETIME,
    feedback_type VARCHAR,
    feedback_mode VARCHAR,
    department VARCHAR,
    call_centre VARCHAR,
    comments TEXT,
    satisfaction_score INT,
    professionalism_score INT,
    response_time_score INT,
    resolution_score INT,
    overall_score INT,
    resolution_provided VARCHAR
);

CREATE TABLE Patient_Database.Patient_Health_Records.Appointment (
    id STRING,
    resourceType STRING,
    created TIMESTAMP,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    description STRING,
    minutesDuration INT,
    status STRING,
    patient_id STRING,
    practitioner_id STRING,
    Pract_type STRING,
    callback_type STRING,
    participant VARIANT,
    raw_data VARIANT
);

CREATE OR REPLACE TABLE Patient_Database.Patient_Health_Records.Condition (
    id STRING,
    resourceType STRING,
    recorded_date TIMESTAMP,
    onset_date TIMESTAMP,
    clinical_status STRING,
    verification_status STRING,
    condition_code STRING, 
    condition_display STRING, 
    severity_code STRING,
    severity_display STRING,
    category STRING,
    patient_id STRING,
    meta_lastUpdated TIMESTAMP,
    raw_data VARIANT
);

CREATE OR REPLACE TABLE Patient_Database.Patient_Health_Records.Endpoint (
    id STRING,
    web_address STRING,
    phone STRING,
    organization_name STRING,
    practice_code STRING,
    start_date DATE,
    end_date DATE,
    raw_data VARIANT
);

CREATE TABLE Patient_Database.Patient_Health_Records.Organization (
    id STRING,
    organization_name STRING,
    address STRING,
    city STRING,
    state STRING,
    country STRING,
    postal_code STRING,
    admin_contact_name STRING,
    phone_number STRING,
    website STRING,
    raw_data VARIANT
);

CREATE TABLE Patient_Database.Patient_Health_Records.Patient (
    id STRING,
    patient_name STRING,
    date_of_birth DATE,
    phone_number STRING, 
    email STRING, 
    marital_status STRING, 
    multiple_birth BOOLEAN,
    organization_name STRING,
    gp_code STRING,
    general_practitioner STRING,
    emergency_contact_name STRING,
    emergency_contact_relationship STRING,
    emergency_contact_number STRING,
    emergency_contact_address STRING,
    communication_language STRING,
    raw_data VARIANT
);

CREATE TABLE Patient_Database.Patient_Health_Records.Practitioner (
    id STRING,
    practitioner_name STRING,
    date_of_birth DATE,
    gender STRING,
    communication_language STRING,
    address STRING,
    city STRING,
    state STRING,
    country STRING,
    postal_code STRING,
    website_address STRING,
    qualification STRING,
    certification_number STRING,
    organization_name STRING,
    gp_code STRING,
    phone STRING,
    email STRING,
    raw_data VARIANT
);


--creating whole record using single staging and querying results through folders
CREATE TABLE Patient_Database.Patient_Health_Records.patient_data_staging (
    folder_name STRING,
    raw_data VARIANT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COPY INTO patient_data_staging (folder_name, raw_data)
FROM (
  SELECT
    METADATA$FILENAME AS folder_name,
    $1 AS raw_data
  FROM @patient_data_stage
)
FILE_FORMAT = (TYPE = 'JSON');


SELECT *
FROM patient_data_staging
WHERE folder_name LIKE '%Appointment/%';


--creating snowpipes for continuous data loading
CREATE PIPE Call_History.call_history_pipe
AUTO_INGEST = TRUE
INTEGRATION = call_notification_integration
AS
COPY INTO Patient_Database.Call_History.patient_call_records
FROM @Call_History.call_history_stage
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*[.]parquet'
ON_ERROR = 'CONTINUE';

CREATE PIPE Feedback_History.feedback_history_pipe
AUTO_INGEST = TRUE
INTEGRATION = feedback_notification_integration
AS
COPY INTO Patient_Database.Feedback_History.patient_feedback_records
FROM @Feedback_History.feedback_history_stage
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*[.]parquet'
ON_ERROR = 'CONTINUE';

CREATE PIPE Patient_Health_Records.appointment_pipe
AUTO_INGEST = TRUE
INTEGRATION = patient_data_integration
AS
COPY INTO Patient_Database.Patient_Health_Records.Appointment
(id, resourceType, created, start_time, end_time, description, minutesDuration, status, 
 patient_id, practitioner_id, Pract_type, callback_type, participant, raw_data)
FROM (
    SELECT
        $1:"id"::STRING AS id,
        $1:"resourceType"::STRING AS resourceType,
        $1:"created"::TIMESTAMP AS created,
        $1:"start"::TIMESTAMP AS start_time,
        $1:"end"::TIMESTAMP AS end_time,
        $1:"description"::STRING AS description,
        $1:"minutesDuration"::INT AS minutesDuration,
        $1:"status"::STRING AS status,
        REGEXP_SUBSTR($1:"participant"[0]:"actor":"reference"::STRING, '[^/]+$') AS patient_id,
        REGEXP_SUBSTR($1:"participant"[1]:"actor":"reference"::STRING, '[^/]+$') AS practitioner_id,
        $1:"participant"[0]:"type"[0]:"coding"[0]:"display"::STRING AS Pract_type,
        $1:"participant"[1]:"type"[0]:"coding"[0]:"display"::STRING AS callback_type,
        $1:"participant" AS participant,
        $1 AS raw_data
    FROM @Patient_Health_Records.appointment_stage
)

CREATE PIPE Patient_Health_Records.condition_pipe
AUTO_INGEST = TRUE
INTEGRATION = patient_data_integration
AS
COPY INTO Patient_Database.Patient_Health_Records.Condition
(id, resourceType, recorded_date, onset_date, clinical_status, verification_status,
 condition_code, condition_display, severity_code, severity_display, category, 
 patient_id, meta_lastUpdated, raw_data)
FROM (
    SELECT
        $1:"id"::STRING AS id,
        $1:"resourceType"::STRING AS resourceType,
        $1:"recordedDate"::TIMESTAMP AS recorded_date,
        $1:"onsetDateTime"::TIMESTAMP AS onset_date,
        $1:"clinicalStatus":"coding"[0]:"code"::STRING AS clinical_status,
        $1:"verificationStatus":"coding"[0]:"code"::STRING AS verification_status,
        $1:"code":"coding"[0]:"code"::STRING AS condition_code,
        $1:"code":"coding"[0]:"display"::STRING AS condition_display,
        $1:"severity":"coding"[0]:"code"::STRING AS severity_code,
        $1:"severity":"coding"[0]:"display"::STRING AS severity_display,
        $1:"category"[0]:"coding"[0]:"code"::STRING AS category,
        REGEXP_SUBSTR($1:"subject":"reference"::STRING, '[^/]+$') AS patient_id,
        $1:"meta":"lastUpdated"::TIMESTAMP AS meta_lastUpdated,
        $1 AS raw_data
    FROM @Patient_Health_Records.condition_stage
)

CREATE PIPE Patient_Health_Records.endpoint_pipe
AUTO_INGEST = TRUE
INTEGRATION = patient_data_integration
AS
COPY INTO Patient_Database.Patient_Health_Records.Endpoint
(id, web_address, phone, organization_name, practice_code, start_date, end_date, raw_data)
FROM (
    SELECT
        $1:"id"::STRING AS id,
        $1:"address"::STRING AS web_address,
        $1:"contact"[0]:"value"::STRING AS phone, 
        $1:"header"[0]::STRING AS organization_name,
        REGEXP_SUBSTR($1:"managingOrganization":"reference"::STRING, '[^/]+$') AS practice_code,
        $1:"period":"start"::DATE AS start_date,
        $1:"period":"end"::DATE AS end_date,
        $1 AS raw_data
    FROM @Patient_Health_Records.endpoint_stage
)

CREATE PIPE Patient_Health_Records.organization_pipe
AUTO_INGEST = TRUE
INTEGRATION = patient_data_integration
AS
COPY INTO Patient_Database.Patient_Health_Records.Organization
(id, organization_name, address, city, state, country, postal_code, admin_contact_name, phone_number, website, raw_data)
FROM (
    SELECT
        $1:"id"::STRING AS id,
        $1:"name"::STRING AS organization_name,
        ARRAY_TO_STRING($1:"contact"[0]:"address":"line", ', ')::STRING AS address,
        $1:"contact"[0]:"address":"city"::STRING AS city, 
        $1:"contact"[0]:"address":"state"::STRING AS state,
        $1:"contact"[0]:"address":"country"::STRING AS country,
        $1:"contact"[0]:"address":"postalCode"::STRING AS postal_code,
        CONCAT($1:"contact"[0]:"name":"given"[0]::STRING, ' ', $1:"contact"[0]:"name":"family"::STRING) AS admin_contact_name,
        $1:"contact"[0]:"telecom"[1]:"value"::STRING AS phone_number,
        $1:"identifier"[0]:"system"::STRING AS website,
        $1 AS raw_data
    FROM @Patient_Health_Records.organization_stage
)

CREATE PIPE Patient_Health_Records.patient_pipe
AUTO_INGEST = TRUE
INTEGRATION = patient_data_integration
AS
COPY INTO Patient_Database.Patient_Health_Records.Patient
(id, patient_name, date_of_birth, phone_number, email, marital_status, multiple_birth, 
 organization_name, gp_code, general_practitioner, emergency_contact_name, 
 emergency_contact_relationship, emergency_contact_number, emergency_contact_address, 
 communication_language, raw_data)
FROM (
    SELECT
        $1:"id"::STRING AS id,
        CONCAT($1:"name"[0]:"prefix"[0]::STRING, ' ', $1:"name"[0]:"given"[0]::STRING, ' ', $1:"name"[0]:"family"::STRING) AS patient_name,
        $1:"birthDate"::DATE AS date_of_birth,
        $1:"telecom"[0]:"value"::STRING AS phone_number,
        $1:"telecom"[1]:"value"::STRING AS email,
        $1:"maritalStatus":"coding"[0]:"display"::STRING AS marital_status,
        $1:"multipleBirthBoolean"::BOOLEAN AS multiple_birth,
        $1:"managingOrganization":"display"::STRING AS organization_name,
        REGEXP_SUBSTR($1:"managingOrganization":"reference"::STRING, '[^/]+$') AS gp_code,
        REGEXP_SUBSTR($1:"generalPractitioner"[0]:"reference"::STRING, '[^/]+$') AS general_practitioner,
        CONCAT($1:"contact"[0]:"name":"given"[0]::STRING, ' ', $1:"contact"[0]:"name":"family"::STRING) AS emergency_contact_name,
        $1:"contact"[0]:"relationship"[0]:"coding"[0]:"display"::STRING AS emergency_contact_relationship,
        $1:"contact"[0]:"telecom"[0]:"value"::STRING AS emergency_contact_number,
        CONCAT(ARRAY_TO_STRING($1:"contact"[0]:"address":"line", ', '), ', ', $1:"contact"[0]:"address":"city"::STRING, ', ', 
               $1:"contact"[0]:"address":"state"::STRING, ', ', $1:"contact"[0]:"address":"country"::STRING, ', ', 
               $1:"contact"[0]:"address":"postalCode"::STRING) AS emergency_contact_address,
        $1:"communication"[0]:"language":"coding"[0]:"display"::STRING AS communication_language,
        $1 AS raw_data
    FROM @Patient_Health_Records.patient_stage
)


CREATE PIPE Patient_Health_Records.practitioner_pipe
AUTO_INGEST = TRUE
INTEGRATION = patient_data_integration
AS
COPY INTO Patient_Database.Patient_Health_Records.Practitioner
(id, practitioner_name, date_of_birth, gender, communication_language, 
 address, city, state, country, postal_code, website_address, 
 qualification, certification_number, organization_name, gp_code, 
 phone, email, raw_data)
FROM (
    SELECT
        $1:"id"::STRING AS id,
        CONCAT($1:"name"[0]:"given"[0]::STRING, ' ', $1:"name"[0]:"family"::STRING) AS practitioner_name,
        $1:"birthDate"::DATE AS date_of_birth,
        $1:"gender"::STRING AS gender,
        $1:"communication"[0]:"coding"[0]:"display"::STRING AS communication_language,
        CONCAT(ARRAY_TO_STRING($1:"address"[0]:"line", ', '), ', ', $1:"address"[0]:"city"::STRING, ', ', 
               $1:"address"[0]:"state"::STRING, ', ', $1:"address"[0]:"country"::STRING, ', ', 
               $1:"address"[0]:"postalCode"::STRING) AS address,
        $1:"address"[0]:"city"::STRING AS city,
        $1:"address"[0]:"state"::STRING AS state,
        $1:"address"[0]:"country"::STRING AS country,
        $1:"address"[0]:"postalCode"::STRING AS postal_code,
        $1:"identifier"[0]:"system"::STRING AS website_address,
        $1:"qualification"[0]:"code":"coding"[0]:"display"::STRING AS qualification,
        $1:"qualification"[0]:"identifier"[0]:"value"::STRING AS certification_number,
        $1:"qualification"[0]:"issuer":"display"::STRING AS organization_name,
        REGEXP_SUBSTR($1:"qualification"[0]:"issuer":"reference"::STRING, '[^/]+$') AS gp_code,
        $1:"telecom"[0]:"value"::STRING AS phone,
        $1:"telecom"[1]:"value"::STRING AS email,
        $1 AS raw_data
    FROM @Patient_Health_Records.practitioner_stage
)

--monitoring the pipe status
SELECT SYSTEM$PIPE_STATUS('Call_History.call_history_pipe');
SELECT SYSTEM$PIPE_STATUS('Patient_Health_Records.patient_pipe');

--force snowpipe to resume
CALL SYSTEM$PIPE_FORCE_RESUME('Call_History.call_history_pipe');
CALL SYSTEM$PIPE_FORCE_RESUME('Patient_Health_Records.appointment_pipe');

--checking copy history to ensure successful loads
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'Call_History.patient_call_records',
    START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
));


--creating stream tables to track incremental changes, and cdc
CREATE STREAM Call_History.call_history_stream ON TABLE Patient_Database.Call_History.patient_call_records;
CREATE STREAM Feedback_History.feedback_history_stream ON TABLE Patient_Database.Feedback_History.patient_feedback_records;
CREATE STREAM Patient_Health_Records.appointment_stream ON TABLE Patient_Database.Patient_Health_Records.Appointment;
CREATE STREAM Patient_Health_Records.condition_stream ON TABLE Patient_Database.Patient_Health_Records.Condition;
CREATE STREAM Patient_Health_Records.endpoint_stream ON TABLE Patient_Database.Patient_Health_Records.Endpoint;
CREATE STREAM Patient_Health_Records.organization_stream ON TABLE Patient_Database.Patient_Health_Records.Organization;
CREATE STREAM Patient_Health_Records.patient_stream ON TABLE Patient_Database.Patient_Health_Records.Patient;
CREATE STREAM Patient_Health_Records.practitioner_stream ON TABLE Patient_Database.Patient_Health_Records.Practitioner;


--creating tables to store processed data
CREATE TABLE Patient_Database.Call_History.processed_patient_call_records AS 
SELECT * FROM Patient_Database.Call_History.patient_call_records WHERE 1=0;

CREATE TABLE Patient_Database.Feedback_History.processed_patient_feedback_records AS 
SELECT * FROM Patient_Database.Feedback_History.patient_feedback_records WHERE 1=0;

CREATE TABLE Patient_Database.Patient_Health_Records.appointment_processed AS 
SELECT * FROM Patient_Database.Patient_Health_Records.Appointment WHERE 1=0;

CREATE TABLE Patient_Database.Patient_Health_Records.condition_processed AS 
SELECT * FROM Patient_Database.Patient_Health_Records.Condition WHERE 1=0;

CREATE TABLE Patient_Database.Patient_Health_Records.endpoint_processed AS 
SELECT * FROM Patient_Database.Patient_Health_Records.Endpoint WHERE 1=0;

CREATE TABLE Patient_Database.Patient_Health_Records.organization_processed AS 
SELECT * FROM Patient_Database.Patient_Health_Records.Organization WHERE 1=0;

CREATE TABLE Patient_Database.Patient_Health_Records.patient_processed AS 
SELECT * FROM Patient_Database.Patient_Health_Records.Patient WHERE 1=0;

CREATE TABLE Patient_Database.Patient_Health_Records.practitioner_processed AS 
SELECT * FROM Patient_Database.Patient_Health_Records.Practitioner WHERE 1=0;


--creating tasks for incremental data processing
CREATE TASK Patient_Database.Call_History.load_call_history_data
   WAREHOUSE = Patient_Management_System
   SCHEDULE = '5 MINUTE'
   WHEN SYSTEM$STREAM_HAS_DATA('Patient_Database.Call_History.call_history_stream')
AS
MERGE INTO Patient_Database.Call_History.processed_patient_call_records t
USING Patient_Database.Public.call_history_stream s
ON t.call_id = s.call_id
WHEN MATCHED AND s.metadata$action = 'DELETE' THEN 
    DELETE
WHEN MATCHED AND s.metadata$action = 'UPDATE' THEN 
    UPDATE SET
        t.patient_id = s.patient_id,
        t.timestamp = s.timestamp,
        t.call_type = s.call_type,
        t.resolution_status = s.resolution_status,
        t.department = s.department,
        t.call_duration = s.call_duration,
        t.notes = s.notes,
        t.follow_up_required = s.follow_up_required,
        t.satisfaction_score = s.satisfaction_score,
        t.call_centre = s.call_centre
WHEN NOT MATCHED AND s.metadata$action = 'INSERT' THEN 
    INSERT (call_id, patient_id, timestamp, call_type, resolution_status, department, 
            call_duration, notes, follow_up_required, satisfaction_score, call_centre)
    VALUES (s.call_id, s.patient_id, s.timestamp, s.call_type, s.resolution_status, s.department, 
            s.call_duration, s.notes, s.follow_up_required, s.satisfaction_score, s.call_centre);

CREATE TASK Patient_Database.Feedback_History.load_feedback_history_data
   WAREHOUSE = Patient_Management_System
   SCHEDULE = '5 MINUTE'
   WHEN SYSTEM$STREAM_HAS_DATA('Patient_Database.Feedback_History.feedback_history_stream')
AS
MERGE INTO Patient_Database.Feedback_History.processed_patient_feedback_records t
USING Patient_Database.Public.feedback_history_stream s
ON t.feedback_id = s.feedback_id
WHEN MATCHED AND s.metadata$action = 'DELETE' THEN 
    DELETE
WHEN MATCHED AND s.metadata$action = 'UPDATE' THEN 
    UPDATE SET
        t.patient_id = s.patient_id,
        t.timestamp = s.timestamp,
        t.feedback_type = s.feedback_type,
        t.feedback_mode = s.feedback_mode,
        t.department = s.department,
        t.call_centre = s.call_centre,
        t.comments = s.comments,
        t.satisfaction_score = s.satisfaction_score,
        t.professionalism_score = s.professionalism_score,
        t.response_time_score = s.response_time_score,
        t.resolution_score = s.resolution_score,
        t.overall_score = s.overall_score,
        t.resolution_provided = s.resolution_provided
WHEN NOT MATCHED AND s.metadata$action = 'INSERT' THEN 
    INSERT (feedback_id, patient_id, timestamp, feedback_type, feedback_mode, department, 
            call_centre, comments, satisfaction_score, professionalism_score, response_time_score, 
            resolution_score, overall_score, resolution_provided)
    VALUES (s.feedback_id, s.patient_id, s.timestamp, s.feedback_type, s.feedback_mode, s.department, 
            s.call_centre, s.comments, s.satisfaction_score, s.professionalism_score, s.response_time_score, 
            s.resolution_score, s.overall_score, s.resolution_provided);

CREATE TASK Patient_Health_Records.load_appointment_data
   WAREHOUSE = Patient_Management_System
   SCHEDULE = '5 MINUTE'
   WHEN SYSTEM$STREAM_HAS_DATA('Patient_Health_Records.appointment_stream')
AS
MERGE INTO Patient_Health_Records.appointment_processed t
USING Patient_Health_Records.appointment_stream s
ON t.id = s.id
WHEN MATCHED AND s.metadata$action = 'DELETE' THEN 
    DELETE
WHEN MATCHED AND s.metadata$action = 'UPDATE' THEN 
    UPDATE SET
        t.resourceType = s.resourceType,
        t.created = s.created,
        t.start_time = s.start_time,
        t.end_time = s.end_time,
        t.description = s.description,
        t.minutesDuration = s.minutesDuration,
        t.status = s.status,
        t.patient_id = s.patient_id,
        t.practitioner_id = s.practitioner_id,
        t.Pract_type = s.Pract_type,
        t.callback_type = s.callback_type,
        t.participant = s.participant,
        t.raw_data = s.raw_data
WHEN NOT MATCHED AND s.metadata$action = 'INSERT' THEN 
    INSERT (id, resourceType, created, start_time, end_time, description, minutesDuration, status, 
            patient_id, practitioner_id, Pract_type, callback_type, participant, raw_data)
    VALUES (s.id, s.resourceType, s.created, s.start_time, s.end_time, s.description, 
            s.minutesDuration, s.status, s.patient_id, s.practitioner_id, 
            s.Pract_type, s.callback_type, s.participant, s.raw_data);

CREATE TASK Patient_Health_Records.load_condition_data
   WAREHOUSE = Patient_Management_System
   SCHEDULE = '5 MINUTE'
   WHEN SYSTEM$STREAM_HAS_DATA('Patient_Health_Records.condition_stream')
AS
MERGE INTO Patient_Health_Records.condition_processed t
USING Patient_Health_Records.condition_stream s
ON t.id = s.id
WHEN MATCHED AND s.metadata$action = 'DELETE' THEN 
    DELETE
WHEN MATCHED AND s.metadata$action = 'UPDATE' THEN 
    UPDATE SET
        t.resourceType = s.resourceType,
        t.recorded_date = s.recorded_date,
        t.onset_date = s.onset_date,
        t.clinical_status = s.clinical_status,
        t.verification_status = s.verification_status,
        t.condition_code = s.condition_code,
        t.condition_display = s.condition_display,
        t.severity_code = s.severity_code,
        t.severity_display = s.severity_display,
        t.category = s.category,
        t.patient_id = s.patient_id,
        t.meta_lastUpdated = s.meta_lastUpdated,
        t.raw_data = s.raw_data
WHEN NOT MATCHED AND s.metadata$action = 'INSERT' THEN 
    INSERT (id, resourceType, recorded_date, onset_date, clinical_status, verification_status,
            condition_code, condition_display, severity_code, severity_display, category, 
            patient_id, meta_lastUpdated, raw_data)
    VALUES (s.id, s.resourceType, s.recorded_date, s.onset_date, s.clinical_status, 
            s.verification_status, s.condition_code, s.condition_display, 
            s.severity_code, s.severity_display, s.category, s.patient_id, 
            s.meta_lastUpdated, s.raw_data);

CREATE TASK Patient_Health_Records.load_endpoint_data
   WAREHOUSE = Patient_Management_System
   SCHEDULE = '5 MINUTE'
   WHEN SYSTEM$STREAM_HAS_DATA('Patient_Health_Records.endpoint_stream')
AS
MERGE INTO Patient_Health_Records.endpoint_processed t
USING Patient_Health_Records.endpoint_stream s
ON t.id = s.id
WHEN MATCHED AND s.metadata$action = 'DELETE' THEN 
    DELETE
WHEN MATCHED AND s.metadata$action = 'UPDATE' THEN 
    UPDATE SET
        t.web_address = s.web_address,
        t.phone = s.phone,
        t.organization_name = s.organization_name,
        t.practice_code = s.practice_code,
        t.start_date = s.start_date,
        t.end_date = s.end_date,
        t.raw_data = s.raw_data
WHEN NOT MATCHED AND s.metadata$action = 'INSERT' THEN 
    INSERT (id, web_address, phone, organization_name, practice_code, 
            start_date, end_date, raw_data)
    VALUES (s.id, s.web_address, s.phone, s.organization_name, 
            s.practice_code, s.start_date, s.end_date, s.raw_data);

CREATE TASK Patient_Health_Records.load_organization_data
   WAREHOUSE = Patient_Management_System
   SCHEDULE = '5 MINUTE'
   WHEN SYSTEM$STREAM_HAS_DATA('Patient_Health_Records.organization_stream')
AS
MERGE INTO Patient_Health_Records.organization_processed t
USING Patient_Health_Records.organization_stream s
ON t.id = s.id
WHEN MATCHED AND s.metadata$action = 'DELETE' THEN 
    DELETE
WHEN MATCHED AND s.metadata$action = 'UPDATE' THEN 
    UPDATE SET
        t.organization_name = s.organization_name,
        t.address = s.address,
        t.city = s.city,
        t.state = s.state,
        t.country = s.country,
        t.postal_code = s.postal_code,
        t.admin_contact_name = s.admin_contact_name,
        t.phone_number = s.phone_number,
        t.website = s.website,
        t.raw_data = s.raw_data
WHEN NOT MATCHED AND s.metadata$action = 'INSERT' THEN 
    INSERT (id, organization_name, address, city, state, country, 
            postal_code, admin_contact_name, phone_number, website, raw_data)
    VALUES (s.id, s.organization_name, s.address, s.city, s.state, 
            s.country, s.postal_code, s.admin_contact_name, s.phone_number, s.website, s.raw_data);

CREATE TASK Patient_Health_Records.load_patient_data
   WAREHOUSE = Patient_Management_System
   SCHEDULE = '5 MINUTE'
   WHEN SYSTEM$STREAM_HAS_DATA('Patient_Health_Records.patient_stream')
AS
MERGE INTO Patient_Health_Records.patient_processed t
USING Patient_Health_Records.patient_stream s
ON t.id = s.id
WHEN MATCHED AND s.metadata$action = 'DELETE' THEN 
    DELETE
WHEN MATCHED AND s.metadata$action = 'UPDATE' THEN 
    UPDATE SET
        t.patient_name = s.patient_name,
        t.date_of_birth = s.date_of_birth,
        t.phone_number = s.phone_number,
        t.email = s.email,
        t.marital_status = s.marital_status,
        t.multiple_birth = s.multiple_birth,
        t.organization_name = s.organization_name,
        t.gp_code = s.gp_code,
        t.general_practitioner = s.general_practitioner,
        t.emergency_contact_name = s.emergency_contact_name,
        t.emergency_contact_relationship = s.emergency_contact_relationship,
        t.emergency_contact_number = s.emergency_contact_number,
        t.emergency_contact_address = s.emergency_contact_address,
        t.communication_language = s.communication_language,
        t.raw_data = s.raw_data
WHEN NOT MATCHED AND s.metadata$action = 'INSERT' THEN 
    INSERT (id, patient_name, date_of_birth, phone_number, email, marital_status, 
            multiple_birth, organization_name, gp_code, general_practitioner, 
            emergency_contact_name, emergency_contact_relationship, 
            emergency_contact_number, emergency_contact_address, communication_language, raw_data)
    VALUES (s.id, s.patient_name, s.date_of_birth, s.phone_number, s.email, s.marital_status, 
            s.multiple_birth, s.organization_name, s.gp_code, s.general_practitioner, 
            s.emergency_contact_name, s.emergency_contact_relationship, 
            s.emergency_contact_number, s.emergency_contact_address, s.communication_language, s.raw_data);

CREATE OR REPLACE TASK Patient_Health_Records.load_practitioner_data
   WAREHOUSE = Patient_Management_System
   SCHEDULE = '5 MINUTE'
   WHEN SYSTEM$STREAM_HAS_DATA('Patient_Health_Records.practitioner_stream')
AS
MERGE INTO Patient_Health_Records.practitioner_processed t
USING Patient_Health_Records.practitioner_stream s
ON t.id = s.id
WHEN MATCHED AND s.metadata$action = 'DELETE' THEN 
    DELETE 
WHEN MATCHED AND s.metadata$action = 'UPDATE' THEN 
    UPDATE SET
        t.practitioner_name = s.practitioner_name,
        t.date_of_birth = s.date_of_birth,
        t.gender = s.gender,
        t.communication_language = s.communication_language,
        t.address = s.address,
        t.city = s.city,
        t.state = s.state,
        t.country = s.country,
        t.postal_code = s.postal_code,
        t.website_address = s.website_address,
        t.qualification = s.qualification,
        t.certification_number = s.certification_number,
        t.organization_name = s.organization_name,
        t.gp_code = s.gp_code,
        t.phone = s.phone,
        t.email = s.email,
        t.raw_data = s.raw_data
WHEN NOT MATCHED AND s.metadata$action = 'INSERT' THEN 
    INSERT (id, practitioner_name, date_of_birth, gender, communication_language, address, city, 
            state, country, postal_code, website_address, qualification, certification_number, 
            organization_name, gp_code, phone, email, raw_data)
    VALUES (s.id, s.practitioner_name, s.date_of_birth, s.gender, s.communication_language, s.address, 
            s.city, s.state, s.country, s.postal_code, s.website_address, s.qualification, 
            s.certification_number, s.organization_name, s.gp_code, s.phone, s.email, s.raw_data);


--enabling the tasks
ALTER TASK Patient_Database.Call_History.load_call_history_data RESUME;
ALTER TASK Patient_Database.Feedback_History.load_feedback_history_data RESUME;
ALTER TASK Patient_Database.Call_History.load_call_history_data RESUME;
ALTER TASK Patient_Database.Feedback_History.load_feedback_history_data RESUME;
ALTER TASK Patient_Health_Records.load_appointment_data RESUME;
ALTER TASK Patient_Health_Records.load_condition_data RESUME;
ALTER TASK Patient_Health_Records.load_endpoint_data RESUME;
ALTER TASK Patient_Health_Records.load_organization_data RESUME;
ALTER TASK Patient_Health_Records.load_patient_data RESUME;
ALTER TASK Patient_Health_Records.load_practitioner_data RESUME;

--checking the status of tasks
SHOW TASKS;


'''
--final check to make sure everything works, truncate and run the dataproc jobs
TRUNCATE TABLE Patient_Database.Call_History.patient_call_records;
TRUNCATE TABLE Patient_Database.Feedback_History.patient_feedback_records;
'''

--QUERIES

--check the call centre with most calls
SELECT 
    call_centre, 
    COUNT(*) AS total_calls
FROM 
    Patient_Database.Call_History.processed_patient_call_records
GROUP BY 
    call_centre
ORDER BY 
    total_calls DESC
LIMIT 1;


--check the practitioner has attended the most appointments (also validate all the practitioner ids are referenced correctly in both table)
SELECT 
    a.practitioner_id, 
    p.practitioner_name, 
    COUNT(*) AS total_appointments
FROM 
    Patient_Database.Patient_Health_Records.appointment_processed a
LEFT JOIN 
    Patient_Database.Patient_Health_Records.practitioner_processed p
ON 
    a.practitioner_id = p.id
GROUP BY 
    a.practitioner_id, 
    p.practitioner_name
ORDER BY 
    total_appointments DESC;

SELECT 
    a.practitioner_id, 
    p.practitioner_name, 
    COUNT(*) AS total_appointments
FROM 
    Patient_Database.Patient_Health_Records.appointment_processed a
INNER JOIN 
    Patient_Database.Patient_Health_Records.practitioner_processed p
ON 
    a.practitioner_id = p.id
GROUP BY 
    a.practitioner_id, 
    p.practitioner_name
ORDER BY 
    total_appointments DESC;


--check average satisfaction score for each call centre based on calls
SELECT 
    call_centre, 
    AVG(satisfaction_score) AS avg_satisfaction_score
FROM 
    Patient_Database.Call_History.processed_patient_call_records
GROUP BY 
    call_centre
ORDER BY 
    avg_satisfaction_score DESC;


-- Find patients with the highest call satisfaction scores who have made more than 10 calls
SELECT 
    p.patient_name,
    AVG(c.satisfaction_score) AS average_satisfaction_score,
    COUNT(c.call_id) AS total_calls
FROM 
    Patient_Database.Patient_Health_Records.Patient p
INNER JOIN 
    Patient_Database.Call_History.patient_call_records c
ON 
    p.id = c.patient_id
WHERE 
    c.satisfaction_score IS NOT NULL
GROUP BY 
    p.patient_name
HAVING 
    COUNT(c.call_id) > 10
ORDER BY 
    average_satisfaction_score DESC, total_calls DESC;