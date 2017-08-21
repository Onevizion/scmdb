# scmdb
Tool to better integrate relational DB with version control systems. 
Executes SQL scripts to keep DB in syncs with current source code version when switching back and force between branches.
Maintains directory of all DB object DDLs and automatically updates it when object modified with new script created by developer.

We asume develpers implementing SQL scripts to upgrade DB and additionally rollback script - to move DB to the initial state.
Ideally each upgrade script should have it's rollback conterpart.
Following is name convention for script files:
```
<script oder num>_<short description>.sql
<script oder num>_<short description>_rollback.sql
```
