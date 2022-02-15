# ST207 - Databases: Project (MT2021)

### Overall objective

This project is intended to i) assess your overall knowledge related to Databases, specifically the concepts and techniques discussed during our lectures and seminars, ii) give you the opportunity to design an entire database application on a topic/scenario of your choice, and iii) allow you to work as a data science team.

### Instructions

1. **GROUPS**: This is a workgroup project. As such, the group is expected to design a solid solution for a particular application or scenario. Everyone in the group is expected to engage and contribute to the final solution.

**Forming groups**: if you need to find someone else to form a group with you, check our [Excel sheet](https://docs.google.com/spreadsheets/d/1_pCprlJkDmkJ09yawhaxpQCWkyb8IhjcNoYKQl2Y_1A/edit?usp=sharing) and contact your prospective teammates. **Groups ideally consist of and are limited to 3 students**.

2. **TOPIC**: Choose a topic or scenario for which you want to design a database application. **See the References section for some ideas**. Make sure your topic/scenario has a good number of entities and real relationships that you can map into your database design. The main point for you is to assess how easy/hard is to design a database application for the chosen topic/scenario given i) your knowledge about the context, ii) the set of queries and other operations you can perform over the proposed database, and iii) available data (see item 3).

3. **DATA**: Make sure you can identify a consistent set of **real data** to use in your application. You can also **generate synthetic data** in case you don't find real data. Also, you can **use any existing dataset(s)** and **import these data into your database** application. Make sure you have a clear understanding of the data and that the data is of good quality/completeness. **No need for playing with big/huge data**, but **make sure you have a good amount of data for each entity/object that allows you for relevant queries and update operations**.

4. **DATA MODELLING**: Design an **Entity-Relationship (ER)** or **Enhanced ER (EER)** model capturing all the entities and relationships, primary and foreign keys, single and multivalued attributes, weak entities, partial and total relationships, and any other important aspect for understanding the context of your database application. You can choose any data modelling tool. If you decide for a **NoSQL database**, make sure you provide a **conceptual description** of the topic, the entities/objects and corresponding structure/attributes, any relationships among the objects, and any other specific aspects you are considering in your model. Make sure you **read Week11's lecture materials on Database Normalization**.

5. **DATABASE CREATION/DESIGN**: Provide a consistent set of DDL commands for creating/materialising your ER/ERR model into a set of tables/relations and corresponding data. Remember to create any **indexes, triggers and views** as needed by the application rules of your topic/scenario. In case of a **NoSQL database**, specify all the creation commands needed for mapping your conceptual model into a set of objects (e.g. documents, graphs etc). This step should cover all the **database definition** tasks.

6. **DATABASE USAGE**: Based on the chosen topic/scenario, specify a set of queries and update operations. **There is no minimum number of operations**, but make sure you address the main operations (usage) of the given database (e.g. a database for a library should provide operations for registering and querying items, users, loans etc, as well as other update and remove operations). Also, make sure to consider whether you can use **aggregation operations, subqueries, and any other type of more elaborated query**. You **must** provide a **consistent textual description** of each query in terms of i) what the query or update operation is about, ii) input parameters and conditions for filtering/matching data, and iii) expected outputs. **Only the SQL/NoSQL code is not sufficient; you must provide the textual description explaining all the database operations**.

7. **DATABASE TECHNOLOGY**: Feel free to play with any database software/tool and/or programming library. Make sure you justify your choice based on the chosen topic/scenario, proposed database operations, and available data.

### Deliverables

Your **solution** `MUST` contain:

* a PDF document with i) LSE candidate numbers (LSE student IDs are fine in case you don't have your candidate numbers, but **don't put your names**), ii) description of the chosen topic/scenario (based on item 2 above), iii) description of your data (based on item 3 above), iv) justification of the database technology/tool (item 7), v) the outputs of your data modelling step (any ER/EER diagram or conceptual description of your NoSQL model), and vi) textual description of all the operations (queries and updates) with the corresponding outputs for each command. Reports are restricted to **8 content pages** (including diagrams and output of SQL commands). Please, **do not include code for creating the database, importing data etc in your report**.

* any code file designed in your project (i.e., SQL commands, scripts for creating the database, importing data etc).

* any dataset or synthetic data used in your project.

### Important dates

* Assignment released: 13/12/2021
* Submission of group/topic information: 17/12/2021, 6 pm (London)
* Submission of solution: 21/01/2022, 6 pm (London)
* Feedback and grade (provisional): 25/02/2022, 10 pm (London)

### Marking criteria

* This assignment is worth 60% of the final grade.
* **IMPORTANT**: according to the School policy, you **must** submit an answer to this assignment; otherwise, you will be graded 0 (zero).

| Problem breakdown  | Max marks |
| ------------- | ------------- |
| (2) Topic/scenario - relevance and complexity of the topic/scenario in terms of entities, relationships, and usage operations. Clear description of the topic/scenario.  | 15 |
| (3) Data - data consistency and quality. Usage of real data and any criteria for subsampling. Generation of synthetic data and how it mimics a real scenario. Good description of the data. Amount of data used versus database usage operations.  | 10 |
| (4) Database modelling - model clarity and consistency (how close to the real scenario it is). Complete description of all entities/objects, relationships, keys and constraints. | 10 |
| (5) Database creation - Complete and correct set of DDL commands for materialising the database model into a set of relations, documents, or nodes, and associated relationships. Use of indexes, views, and triggers according to the application rules of the chosen topic/scenario. | 15 |
| (6) Database usage - relevant and consistent set of queries and update operations. The rationale behind each database operation and to what extent the provided query/update commands have explored the available data. Usage of aggregation, subquery, join and other complex query/update structure. If available in the database, good exploration of indexes, views, and triggers. Clear documentation of all outputs. | 30  |
| (7) Database technology - justification, adherence, and technical complexity involved in its use. | 10 |
| Documentation - quality of the PDF report, code organisation and documentation. | 10 |
| TOTAL  | 100  |

### Feedback and grade

* To be provided after your submission.

### References

- Wikipedia: [Database application](https://en.wikipedia.org/wiki/Database_application)
- [10 Database Examples in Real Life](https://www.liquidweb.com/blog/ten-ways-databases-run-your-life/)
- [Database Applications Types and Examples](https://www.mongodb.com/basics/database-application)
- [15 Database Software Examples 2021](https://rigorousthemes.com/blog/database-software-examples/)
- Oracle: [Application Express App Builder User's Guide](https://docs.oracle.com/database/apex-5.1/HTMDB/understanding-sample-database-application.htm#HTMDB02005)
- Microsoft: [Database design basics](https://support.microsoft.com/en-us/office/database-design-basics-eb2159cf-1e30-401a-8084-bd4f9c9ca1f5)
- Xplenty: [Complete Guide to Database Schema Design](https://www.xplenty.com/blog/complete-guide-to-database-schema-design-guide/)
- Medium: [10 Best Database Design Practices](https://medium.com/quick-code/10-best-database-design-practices-1f10f3441730)
- Learn Computer Science: [How to design a database?](https://www.learncomputerscienceonline.com/how-to-design-database/)
- The Digital Skye: [8 Practical Guidelines for Designing Databases That Don’t Land You in Hot Water](https://thedigitalskye.com/2020/12/19/8-practical-guidelines-for-designing-databases-that-dont-land-you-in-hot-water/)
