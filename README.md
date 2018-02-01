# STAAR-TELPAS-Data-Import
My first project with T-shirt Size "L" at Frontline Education



v3: TELPAS and STAAR Import
Jira Ticket: SEISER-2698 (https://frontlinetechnologies.atlassian.net/browse/SEISER-2698)


Context:
The Texas Education Agency (TEA) requires students to take the STAAR test each year.  In addition, all students classified as 1 - LEP (Limited English Proficient) must take the TELPAS assessment.  When results are returned to the district, they are used for a variety of purposes including consideration for exit for students with Limited English Proficiency.
Current Behavior
•   The system supports the addition of assessment records through the following methods:
•   Integration
•   Data Import
•   Manual Addition
•   Addition of data:
•   When data is added via an integration, the system wipes and replaces all data (including manually added data) [Note: this implies that the SIS is the single source of truth for data]
•   Data Import


•   The Data Import specification allows a user to define a delimited file definition [Note: this requires that users transform the .dat file into an accepted delimited file type]
•   Current Pain Points and Workarounds:
•   Users take screenshots of Eduphoria assessment results and paste them into the rich text field to include the data for review
•   Users who manually enter data "lose their work" when an integration sync occurs
•   Administrative users must manually reformat the .dat file into a supported file format; this is a cumbersome step and often is a point of failure for many districts; where this is the case, teachers may be enlisted to manually add data


Desired Behavior: Acceptance Criteria
•   The system shall support the addition of assessment records through the following methods:
•   Integration (Current)
•   Data Import v1 (Current)
•   Data Import v2 (.dat file) New
•   Manual addition (Current)
•   Create Data Import v2 to support 2017 STAAR Testing .dat file
•   The system shall provide an interface for the user to upload a .dat file consistent with the STAAR .dat file
•   The system shall import the .dat file
•   The system shall pull the following fields from the .dat file:
•   Student Identifier (to match eStar record)
•   Test Type
•   Test Year
•   Test Month
•   Test Day
•   Test Language 
•   STAAR Subject
•   STAAR Grade
•   STAAR Scale Score
•   STAAR Score Code
•   STAAR Result
•   STAAR Accommodations
•   Create Data Import v2 to support 2017 TELPAS Testing .dat file
•   The system shall provide an interface for the user to upload a .dat file consistent with the TELPAS .dat file
•   The system shall import the .dat file
•   The system shall pull the following fields from the .dat file:
•   Student Identifier (to match eStar record)
•   Test Type
•   Test Year
•   Test Month
•   Test Day
•   Listening Proficiency
•   Speaking Proficiency
•   Reading Proficiency
•   Writing Proficiency
•   Composite Score
•   Adding Records
•   A duplicate is defined as two records with the same student, same test, same year, same subject, same window, same scale score
•   An 'update' record is defined as two records with the same student, same test, same year, same subject, same window, different score
•   When the system attempts to add a new record, the system shall check to determine if the record is a duplicate or an 'update'
•   If the record is a duplicate, the system shall skip the record and log an error
•   If the record is an 'update', the system shall update the record with the latest data
•   If a user has manually entered a duplicate record, the system shall display an error in the interface
•   If the record is unique, the system shall add the record to the student's record (note: this should be made available to all products - ARD, 504, RTI, LPAC etc.)
•   Editing Existing Records
•   The system shall allow a user to manually edit an existing record, regardless of its origin (e.g. integration, data import v1, data import v2, manual addition)
•   When an existing record is edited, the system shall save the new record
•   If an integration or data import is performed after the record is edited, the system shall follow the logic stated above (in the event that the record is considered an 'update', the system shall update the record)
•   Support Maintainable Changes in Data Import Capability


•   Note: Each year, the .dat file is likely to change.  The design of the system should account for this known variation.  For example, providing an interface for an internal team member to define a data mapping (e.g. Listening Proficiency Rating field is at position 233 to 233, value 0 = No rating available, value 1 = Beginning, value 2 = Intermediate, value 3 = Advanced, value 4 = Advanced High)
•   State Assessment Screen (? This was an idea suggested at team meeting to implement a State Assessment screen across all products)
•   The system shall provide an interface to view State Assessment scores
•   The system shall display all STAAR records
•   The system shall display all TELPAS records
•   LPAC
•   If a STAAR record is available for a student, the system shall display it in the STAAR Tests screen as a record
•   If a TELPAS record is available for a student, the system shall display it in the TELPAS Tests screen as a record
•   The system shall automatically pull the STAAR testing data into the EOY screen
•   The system shall automatically pull the TELPAS testing data into the EOY screen
•   The system shall automatically pull the STAAR testing data into the Progress Reports screen
•   The system shall automatically pull the TELPAS testing data into the Progress Reports screen
