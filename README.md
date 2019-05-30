# scmdb
Tool to better integrate relational DB with version control systems. 
Executes SQL scripts to keep DB in syncs with current source code version when switching back and force between branches.
Maintains directory of all DB object DDLs and automatically updates it when object modified with new script created by developer.

We assume developers implementing SQL scripts to upgrade DB and additionally rollback script - to move DB to the initial state.
Ideally each upgrade script should have it's rollback counterpart.
Following is name convention for script files:
```
<script oder num>_<short description>.sql
<script oder num>_<short description>_rollback.sql
```

## Supported command line options:
**d** gen-ddl  
Genrate DDLs for new scripts

**a** all  

**n** no-color  
Do not apply color decorations to the console output

**o** omit-changed  
Do not check for sciprt changes. Script modifications detection is based on hash code calc, omiting this procedure may improove perfomance
