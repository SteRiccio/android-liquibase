package liquibase.change.core

import liquibase.change.ChangeStatus
import liquibase.change.StandardChangeTest
import liquibase.changelog.ChangeSet
import liquibase.database.DatabaseConnection
import liquibase.database.DatabaseFactory
import liquibase.database.PreparedStatementFactory
import liquibase.database.core.MSSQLDatabase
import liquibase.database.core.MockDatabase
import liquibase.database.jvm.JdbcConnection
import liquibase.exception.DatabaseException
import liquibase.parser.core.ParsedNodeException
import liquibase.resource.ResourceAccessor
import liquibase.snapshot.MockSnapshotGeneratorFactory
import liquibase.snapshot.SnapshotGeneratorFactory
import liquibase.statement.ExecutablePreparedStatement
import liquibase.statement.ExecutablePreparedStatementBase
import liquibase.statement.ExecutablePreparedStatementBaseTest
import liquibase.statement.SqlStatement
import liquibase.statement.core.InsertSetStatement
import liquibase.statement.core.InsertStatement
import liquibase.test.TestContext
import spock.lang.Unroll

import java.sql.PreparedStatement
import java.sql.SQLException

public class LoadDataChangeTest extends StandardChangeTest {

    MSSQLDatabase mssqlDb;
    MockDatabase mockDb;

    def setup() {
        ResourceAccessor resourceAccessor = TestContext.getInstance().getTestResourceAccessor()
        String offlineUrl

        mssqlDb = new MSSQLDatabase();
        mssqlDb.setConnection(DatabaseFactory.getInstance().openConnection("offline:mssql",
                "superuser", "superpass", null, resourceAccessor));

        mockDb = new MockDatabase();
        mockDb.setConnection((DatabaseConnection) null)
    }


    def "loadDataEmpty database agnostic"() throws Exception {
        when:
        LoadDataChange refactoring = new LoadDataChange();
        refactoring.setSchemaName("SCHEMA_NAME");
        refactoring.setTableName("TABLE_NAME");
        refactoring.setFile("liquibase/change/core/empty.data.csv");
        refactoring.setSeparator(",");

        SqlStatement[] sqlStatement = refactoring.generateStatements(mssqlDb);
        then:
        sqlStatement.length == 0
    }

    def "loadDataEmpty not using InsertSetStatement"() throws Exception {
        when:
        LoadDataChange refactoring = new LoadDataChange();
        refactoring.setSchemaName("SCHEMA_NAME");
        refactoring.setTableName("TABLE_NAME");
        refactoring.setFile("liquibase/change/core/empty.data.csv");
        refactoring.setSeparator(",");

        SqlStatement[] sqlStatements = refactoring.generateStatements(mockDb);

        then:
        sqlStatements.length == 0
    }


    @Unroll("multiple formats with the same data for #fileName")
    def "multiple formats with the same data using InsertSetStatement"() throws Exception {
        when:
        LoadDataChange refactoring = new LoadDataChange();
        refactoring.setSchemaName("SCHEMA_NAME");
        refactoring.setTableName("TABLE_NAME");
        refactoring.setFile(fileName);
        if (separator != null) {
            refactoring.setSeparator(separator);
        }
        if (quotChar != null) {
            refactoring.setQuotchar(quotChar);
        }

        SqlStatement[] sqlStatement = refactoring.generateStatements(new MSSQLDatabase());
        then:
        sqlStatement.length == 1
        assert sqlStatement[0] instanceof InsertSetStatement

        when:
        SqlStatement[] sqlStatements = ((InsertSetStatement) sqlStatement[0]).getStatementsArray();

        then:
        sqlStatements.length == 2
        assert sqlStatements[0] instanceof InsertStatement
        assert sqlStatements[1] instanceof InsertStatement

        "SCHEMA_NAME" == ((InsertStatement) sqlStatements[0]).getSchemaName()
        "TABLE_NAME" == ((InsertStatement) sqlStatements[0]).getTableName()
        "Bob Johnson" == ((InsertStatement) sqlStatements[0]).getColumnValue("name")
        "bjohnson" == ((InsertStatement) sqlStatements[0]).getColumnValue("username")

        "SCHEMA_NAME" == ((InsertStatement) sqlStatements[1]).getSchemaName()
        "TABLE_NAME" == ((InsertStatement) sqlStatements[1]).getTableName()
        "John Doe" == ((InsertStatement) sqlStatements[1]).getColumnValue("name")
        "jdoe" == ((InsertStatement) sqlStatements[1]).getColumnValue("username")

        where:
        fileName                                    | separator | quotChar
        "liquibase/change/core/sample.data1.tsv"    | "\t"      | null
        "liquibase/change/core/sample.quotchar.tsv" | "\t"      | "'"
        "liquibase/change/core/sample.data1.csv"    | ","       | null
        "liquibase/change/core/sample.data1.csv"    | null      | null
    }

    @Unroll("multiple formats with the same data for #fileName")
    def "multiple formats with the same data not using InsertSetStatement"() throws Exception {
        when:
        LoadDataChange refactoring = new LoadDataChange();
        refactoring.setSchemaName("SCHEMA_NAME");
        refactoring.setTableName("TABLE_NAME");
        refactoring.setFile(fileName);
        if (separator != null) {
            refactoring.setSeparator(separator);
        }
        if (quotChar != null) {
            refactoring.setQuotchar(quotChar);
        }

        SqlStatement[] sqlStatements = refactoring.generateStatements(mockDb);

        then:
        sqlStatements.length == 2
        assert sqlStatements[0] instanceof InsertStatement
        assert sqlStatements[1] instanceof InsertStatement

        "SCHEMA_NAME" == ((InsertStatement) sqlStatements[0]).getSchemaName()
        "TABLE_NAME" == ((InsertStatement) sqlStatements[0]).getTableName()
        "Bob Johnson" == ((InsertStatement) sqlStatements[0]).getColumnValue("name")
        "bjohnson" == ((InsertStatement) sqlStatements[0]).getColumnValue("username")

        "SCHEMA_NAME" == ((InsertStatement) sqlStatements[1]).getSchemaName()
        "TABLE_NAME" == ((InsertStatement) sqlStatements[1]).getTableName()
        "John Doe" == ((InsertStatement) sqlStatements[1]).getColumnValue("name")
        "jdoe" == ((InsertStatement) sqlStatements[1]).getColumnValue("username")

        where:
        fileName | separator | quotChar
        "liquibase/change/core/sample.data1.tsv" | "\t" | null
        "liquibase/change/core/sample.quotchar.tsv" | "\t" | "'"
        "liquibase/change/core/sample.data1.csv" | "," | null
        "liquibase/change/core/sample.data1.csv" | null | null
    }

    def "generateStatement_excel using InsertSetStatement"() throws Exception {
        when:
        LoadDataChange refactoring = new LoadDataChange();
        refactoring.setSchemaName("SCHEMA_NAME");
        refactoring.setTableName("TABLE_NAME");
        refactoring.setFile("liquibase/change/core/sample.data1-excel.csv");

        LoadDataColumnConfig ageConfig = new LoadDataColumnConfig();
        ageConfig.setHeader("age");
        ageConfig.setType("NUMERIC");
        refactoring.addColumn(ageConfig);

        LoadDataColumnConfig activeConfig = new LoadDataColumnConfig();
        activeConfig.setHeader("active");
        activeConfig.setType("BOOLEAN");
        refactoring.addColumn(activeConfig);

        SqlStatement[] sqlStatement = refactoring.generateStatements(new MSSQLDatabase());
        then:
        sqlStatement.length == 1
        assert sqlStatement[0] instanceof InsertSetStatement

        when:
        SqlStatement[] sqlStatements = ((InsertSetStatement) sqlStatement[0]).getStatementsArray();

        then:
        sqlStatements.length == 2
        assert sqlStatements[0] instanceof InsertStatement
        assert sqlStatements[1] instanceof InsertStatement

        "SCHEMA_NAME" == ((InsertStatement) sqlStatements[0]).getSchemaName()
        "TABLE_NAME" == ((InsertStatement) sqlStatements[0]).getTableName()
        "Bob Johnson" == ((InsertStatement) sqlStatements[0]).getColumnValue("name")
        "bjohnson" == ((InsertStatement) sqlStatements[0]).getColumnValue("username")
        "15" == ((InsertStatement) sqlStatements[0]).getColumnValue("age").toString()
        Boolean.TRUE == ((InsertStatement) sqlStatements[0]).getColumnValue("active")

        "SCHEMA_NAME" == ((InsertStatement) sqlStatements[1]).getSchemaName()
        "TABLE_NAME" == ((InsertStatement) sqlStatements[1]).getTableName()
        "John Doe" == ((InsertStatement) sqlStatements[1]).getColumnValue("name")
        "jdoe" == ((InsertStatement) sqlStatements[1]).getColumnValue("username")
        "21" == ((InsertStatement) sqlStatements[1]).getColumnValue("age").toString()
        Boolean.FALSE == ((InsertStatement) sqlStatements[1]).getColumnValue("active")
    }

    def "generateStatement_excel not using InsertStatement"() throws Exception {
        when:
        LoadDataChange refactoring = new LoadDataChange();
        refactoring.setSchemaName("SCHEMA_NAME");
        refactoring.setTableName("TABLE_NAME");
        refactoring.setFile("liquibase/change/core/sample.data1-excel.csv");

        LoadDataColumnConfig ageConfig = new LoadDataColumnConfig();
        ageConfig.setHeader("age");
        ageConfig.setType("NUMERIC");
        refactoring.addColumn(ageConfig);

        LoadDataColumnConfig activeConfig = new LoadDataColumnConfig();
        activeConfig.setHeader("active");
        activeConfig.setType("BOOLEAN");
        refactoring.addColumn(activeConfig);

        SqlStatement[] sqlStatements = refactoring.generateStatements(mockDb);

        then:
        sqlStatements.length == 2
        assert sqlStatements[0] instanceof InsertStatement
        assert sqlStatements[1] instanceof InsertStatement

        "SCHEMA_NAME" == ((InsertStatement) sqlStatements[0]).getSchemaName()
        "TABLE_NAME" == ((InsertStatement) sqlStatements[0]).getTableName()
        "Bob Johnson" == ((InsertStatement) sqlStatements[0]).getColumnValue("name")
        "bjohnson" == ((InsertStatement) sqlStatements[0]).getColumnValue("username")
        "15" == ((InsertStatement) sqlStatements[0]).getColumnValue("age").toString()
        Boolean.TRUE == ((InsertStatement) sqlStatements[0]).getColumnValue("active")

        "SCHEMA_NAME" == ((InsertStatement) sqlStatements[1]).getSchemaName()
        "TABLE_NAME" == ((InsertStatement) sqlStatements[1]).getTableName()
        "John Doe" == ((InsertStatement) sqlStatements[1]).getColumnValue("name")
        "jdoe" == ((InsertStatement) sqlStatements[1]).getColumnValue("username")
        "21" == ((InsertStatement) sqlStatements[1]).getColumnValue("age").toString()
        Boolean.FALSE == ((InsertStatement) sqlStatements[1]).getColumnValue("active")
    }
    
        def "generateStatement_uuid not using InsertStatement"() throws Exception {
        when:
        LoadDataChange refactoring = new LoadDataChange();
        refactoring.setSchemaName("SCHEMA_NAME");
        refactoring.setTableName("TABLE_NAME");
        refactoring.setFile("liquibase/change/core/sample.data3.csv");

        LoadDataColumnConfig idConfig = new LoadDataColumnConfig();
        idConfig.setHeader("id");
        idConfig.setType("UUID");
        refactoring.addColumn(idConfig);

        LoadDataColumnConfig parentIdConfig = new LoadDataColumnConfig();
        parentIdConfig.setHeader("parent_id");
        parentIdConfig.setType("UUID");
        refactoring.addColumn(parentIdConfig);

        SqlStatement[] sqlStatements = refactoring.generateStatements(mockDb);

        then:
        sqlStatements.length == 2
        assert sqlStatements[0] instanceof InsertStatement
        assert sqlStatements[1] instanceof InsertStatement
        println sqlStatements[0]
        println sqlStatements[1]
        
        "SCHEMA_NAME" == ((InsertStatement) sqlStatements[0]).getSchemaName()
        "TABLE_NAME" == ((InsertStatement) sqlStatements[0]).getTableName()
        "c7ac2480-bc96-11e2-a300-64315073a768" == ((InsertStatement) sqlStatements[0]).getColumnValue("id")
        "NULL" == ((InsertStatement) sqlStatements[0]).getColumnValue("parent_id")
        
        "SCHEMA_NAME" == ((InsertStatement) sqlStatements[1]).getSchemaName()
        "TABLE_NAME" == ((InsertStatement) sqlStatements[1]).getTableName()
        "c801be90-bc96-11e2-a300-64315073a768" == ((InsertStatement) sqlStatements[1]).getColumnValue("id")
        "3c39ee40-ac78-11e4-aca7-78acc0c3521f" == ((InsertStatement) sqlStatements[1]).getColumnValue("parent_id")
    }


    def getConfirmationMessage() throws Exception {
        when:
        LoadDataChange refactoring = new LoadDataChange();
        refactoring.setTableName("TABLE_NAME");
        refactoring.setFile("FILE_NAME");

        then:
        "Data loaded from FILE_NAME into TABLE_NAME" == refactoring.getConfirmationMessage()
    }

    def "generateChecksum produces different values with each field"() {
        when:
        LoadDataChange refactoring = new LoadDataChange();
        refactoring.setSchemaName("SCHEMA_NAME");
        refactoring.setTableName("TABLE_NAME");
        refactoring.setFile("liquibase/change/core/sample.data1.csv");

        String md5sum1 = refactoring.generateCheckSum().toString();

        refactoring.setFile("liquibase/change/core/sample.data2.csv");
        String md5sum2 = refactoring.generateCheckSum().toString();

        then:
        assert !md5sum1.equals(md5sum2)
        refactoring.generateCheckSum().toString() == md5sum2
    }

    @Override
    protected boolean canUseStandardGenerateCheckSumTest() {
        return false
    }

    def "checkStatus"() {
        when:
        def snapshotFactory = new MockSnapshotGeneratorFactory()
        SnapshotGeneratorFactory.instance = snapshotFactory

        def change = new LoadDataChange()

        then:
        assert change.checkStatus(mockDb).status == ChangeStatus.Status.unknown
        assert change.checkStatus(mockDb).message == "Cannot check loadData status"
    }

    def "load works"() {
        when:
        def change = new LoadDataChange()
        try {
            change.load(new liquibase.parser.core.ParsedNode(null, "loadData").setValue([
                    [column: [name: "id"]],
                    [column: [name: "new_col", header: "new_col_header"]],
            ]), resourceSupplier.simpleResourceAccessor)
        } catch (ParsedNodeException e) {
            e.printStackTrace()
        }

        then:
        change.columns.size() == 2
        change.columns[0].name == "id"
        change.columns[0].header == null

        change.columns[1].name == "new_col"
        change.columns[1].header == "new_col_header"
    }

    def "relativeToChangelogFile works"() throws Exception {
        when:
        ChangeSet changeSet = new ChangeSet(null, null, true, false,
                "liquibase/empty.changelog.xml",
                null, null, false, null, null);

        LoadDataChange relativeChange = new LoadDataChange();

        relativeChange.setSchemaName("SCHEMA_NAME");
        relativeChange.setTableName("TABLE_NAME");
        relativeChange.setRelativeToChangelogFile(Boolean.TRUE);
        relativeChange.setChangeSet(changeSet);
        relativeChange.setFile("change/core/sample.data1.csv");

        SqlStatement[] relativeStatements = relativeChange.generateStatements(mockDb);

        LoadUpdateDataChange nonRelativeChange = new LoadUpdateDataChange();
        nonRelativeChange.setSchemaName("SCHEMA_NAME");
        nonRelativeChange.setTableName("TABLE_NAME");
        nonRelativeChange.setChangeSet(changeSet);
        nonRelativeChange.setFile("liquibase/change/core/sample.data1.csv");

        SqlStatement[] nonRelativeStatements = nonRelativeChange.generateStatements(mockDb);

        then:
        assert relativeStatements != null
        assert nonRelativeStatements != null
        assert relativeStatements.size() == nonRelativeStatements.size()
    }

    def "checksum does not change when no comments in CSV and comment property changes"() {
        when:
        LoadDataChange refactoring = new LoadDataChange();
        refactoring.setSchemaName("SCHEMA_NAME");
        refactoring.setTableName("TABLE_NAME");
        refactoring.setFile("liquibase/change/core/sample.data1.csv");

        refactoring.setCommentLineStartsWith("") //comments disabled
        String md5sum1 = refactoring.generateCheckSum().toString();

        refactoring.setCommentLineStartsWith("#");
        String md5sum2 = refactoring.generateCheckSum().toString();

        then:
        assert md5sum1.equals(md5sum2)
    }
    def "checksum changes when there are comments in CSV"() {
        when:
        LoadDataChange refactoring = new LoadDataChange();
        refactoring.setSchemaName("SCHEMA_NAME");
        refactoring.setTableName("TABLE_NAME");
        refactoring.setFile("liquibase/change/core/sample.data1-withComments.csv");

        refactoring.setCommentLineStartsWith("") //comments disabled
        String md5sum1 = refactoring.generateCheckSum().toString();

        refactoring.setCommentLineStartsWith("#");
        String md5sum2 = refactoring.generateCheckSum().toString();

        then:
        assert !md5sum1.equals(md5sum2)
    }

    def "checksum same for CSV files with comments and file with removed comments manually"() {
        when:
        LoadDataChange refactoring = new LoadDataChange();
        refactoring.setSchemaName("SCHEMA_NAME");
        refactoring.setTableName("TABLE_NAME");
        refactoring.setFile("liquibase/change/core/sample.data1-withComments.csv");

        refactoring.setCommentLineStartsWith("#");
        String md5sum1 = refactoring.generateCheckSum().toString();

        refactoring.setFile("liquibase/change/core/sample.data1-removedComments.csv");
        refactoring.setCommentLineStartsWith(""); //disable comments just in case
        String md5sum2 = refactoring.generateCheckSum().toString();

        then:
        assert md5sum1.equals(md5sum2)
    }

    def "usePreparedStatements set to false produces InsertSetStatement"() throws Exception {
        when:
        LoadDataChange loadDataChange = new LoadDataChange();
        loadDataChange.setSchemaName("SCHEMA_NAME");
        loadDataChange.setTableName("TABLE_NAME");
        loadDataChange.setUsePreparedStatements(Boolean.FALSE);
        loadDataChange.setFile("liquibase/change/core/sample.data1.csv");

        SqlStatement[] sqlStatement = loadDataChange.generateStatements(new MSSQLDatabase());

        then:
        sqlStatement.length == 1
        assert sqlStatement[0] instanceof InsertSetStatement

        when:
        SqlStatement[] sqlStatements = ((InsertSetStatement) sqlStatement[0]).getStatementsArray();

        then:
        sqlStatements.length == 2
        assert sqlStatements[0] instanceof InsertStatement
        assert sqlStatements[1] instanceof InsertStatement

        "SCHEMA_NAME" == ((InsertStatement) sqlStatements[0]).getSchemaName()
        "TABLE_NAME" == ((InsertStatement) sqlStatements[0]).getTableName()
        "Bob Johnson" == ((InsertStatement) sqlStatements[0]).getColumnValue("name")
        "bjohnson" == ((InsertStatement) sqlStatements[0]).getColumnValue("username")

        "SCHEMA_NAME" == ((InsertStatement) sqlStatements[1]).getSchemaName()
        "TABLE_NAME" == ((InsertStatement) sqlStatements[1]).getTableName()
        "John Doe" == ((InsertStatement) sqlStatements[1]).getColumnValue("name")
        "jdoe" == ((InsertStatement) sqlStatements[1]).getColumnValue("username")
    }
    def "usePreparedStatements set to true produces PreparedStatement"() throws Exception {
        when:
        LoadDataChange loadDataChange = new LoadDataChange();
        loadDataChange.setSchemaName("SCHEMA_NAME");
        loadDataChange.setTableName("TABLE_NAME");
        loadDataChange.setUsePreparedStatements(Boolean.TRUE);
        loadDataChange.setFile("liquibase/change/core/sample.data1.csv");

        SqlStatement[] sqlStatement = loadDataChange.generateStatements(new MSSQLDatabase() { public boolean supportsBatchUpdates() { return true; } });

        then:
        sqlStatement.length == 1
        assert sqlStatement[0] instanceof ExecutablePreparedStatement

        when:
        SqlStatement[] sqlStatements = ((ExecutablePreparedStatement) sqlStatement[0]).getIndividualStatements();
        def conn = new JdbcConnection() {
            def passedInput = "localhost"

            @Override
            protected String getConnectionUrl() throws SQLException {
                return passedInput
            }

            @Override
            PreparedStatement prepareStatement(String sql) throws DatabaseException {
                ExecutablePreparedStatementBaseTest.DummyPreparedStatement stmt = new ExecutablePreparedStatementBaseTest.DummyPreparedStatement()
                return stmt
            }
        }

        def factory = new PreparedStatementFactory(conn)
        ((ExecutablePreparedStatementBase)sqlStatements[0]).execute(factory)

        then:
        sqlStatements.length == 2
        assert sqlStatements[0] instanceof ExecutablePreparedStatement
        assert sqlStatements[1] instanceof ExecutablePreparedStatement

        "SCHEMA_NAME" == ((ExecutablePreparedStatementBase) sqlStatements[0]).getSchemaName()
        "TABLE_NAME" == ((ExecutablePreparedStatementBase) sqlStatements[0]).getTableName()


        "SCHEMA_NAME" == ((ExecutablePreparedStatementBase) sqlStatements[1]).getSchemaName()
        "TABLE_NAME" == ((ExecutablePreparedStatementBase) sqlStatements[1]).getTableName()

    }
    def "DB NO Batch Update Support usePrepared True produces InsertSetStatement"() throws Exception {
        when:
        LoadDataChange loadDataChange = new LoadDataChange();
        loadDataChange.setSchemaName("SCHEMA_NAME");
        loadDataChange.setTableName("TABLE_NAME");
        loadDataChange.setUsePreparedStatements(Boolean.TRUE);
        loadDataChange.setFile("liquibase/change/core/sample.data1.csv");

        SqlStatement[] sqlStatement = loadDataChange.generateStatements(new MSSQLDatabase());

        then:
        sqlStatement.length == 1
        assert sqlStatement[0] instanceof InsertSetStatement

        when:
        SqlStatement[] sqlStatements = ((InsertSetStatement) sqlStatement[0]).getStatementsArray();

        then:
        sqlStatements.length == 2
        assert sqlStatements[0] instanceof InsertStatement
        assert sqlStatements[1] instanceof InsertStatement

        "SCHEMA_NAME" == ((InsertStatement) sqlStatements[0]).getSchemaName()
        "TABLE_NAME" == ((InsertStatement) sqlStatements[0]).getTableName()
        "Bob Johnson" == ((InsertStatement) sqlStatements[0]).getColumnValue("name")
        "bjohnson" == ((InsertStatement) sqlStatements[0]).getColumnValue("username")

        "SCHEMA_NAME" == ((InsertStatement) sqlStatements[1]).getSchemaName()
        "TABLE_NAME" == ((InsertStatement) sqlStatements[1]).getTableName()
        "John Doe" == ((InsertStatement) sqlStatements[1]).getColumnValue("name")
        "jdoe" == ((InsertStatement) sqlStatements[1]).getColumnValue("username")
    }
    def "DB Batch Update Support usePrepared False produces InsertSetStatement"() throws Exception {
        when:
        LoadDataChange loadDataChange = new LoadDataChange();
        loadDataChange.setSchemaName("SCHEMA_NAME");
        loadDataChange.setTableName("TABLE_NAME");
        loadDataChange.setUsePreparedStatements(Boolean.FALSE);
        loadDataChange.setFile("liquibase/change/core/sample.data1.csv");

        SqlStatement[] sqlStatement = loadDataChange.generateStatements(new MSSQLDatabase() { public boolean supportsBatchUpdates() { return true; } });

        then:
        sqlStatement.length == 1
        assert sqlStatement[0] instanceof InsertSetStatement

        when:
        SqlStatement[] sqlStatements = ((InsertSetStatement) sqlStatement[0]).getStatementsArray();

        then:
        sqlStatements.length == 2
        assert sqlStatements[0] instanceof InsertStatement
        assert sqlStatements[1] instanceof InsertStatement

        "SCHEMA_NAME" == ((InsertStatement) sqlStatements[0]).getSchemaName()
        "TABLE_NAME" == ((InsertStatement) sqlStatements[0]).getTableName()
        "Bob Johnson" == ((InsertStatement) sqlStatements[0]).getColumnValue("name")
        "bjohnson" == ((InsertStatement) sqlStatements[0]).getColumnValue("username")

        "SCHEMA_NAME" == ((InsertStatement) sqlStatements[1]).getSchemaName()
        "TABLE_NAME" == ((InsertStatement) sqlStatements[1]).getTableName()
        "John Doe" == ((InsertStatement) sqlStatements[1]).getColumnValue("name")
        "jdoe" == ((InsertStatement) sqlStatements[1]).getColumnValue("username")
    }
}
