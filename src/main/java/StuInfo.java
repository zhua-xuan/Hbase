import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class StuInfo {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    public void init () {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","localhost");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        try{
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    public void close() {
        try{
            if(admin != null) {
                admin.close();
            }
            if (null != connection) {
                connection.close();
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void listTables() throws IOException {
        HTableDescriptor[] tableDescriptor =admin.listTables();
        if(tableDescriptor.length == 0){
            System.out.println("no table.");
        }
        else{
            System.out.println("Tables:");
            for (int i=0; i<tableDescriptor.length; i++ ){
                System.out.println(tableDescriptor[i].getNameAsString());
            }
        }

    }

    public void createTable(String myTableName, String[] colFamily) throws IOException {
        TableName tableName = TableName.valueOf(myTableName);
        if(admin.tableExists(tableName)){
            System.out.println("table "+ myTableName +" exists!");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for(String str:colFamily){
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
            System.out.println("Successfully create table: "+ myTableName +"!");
        }
    }


    public void scanTable(String myTableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(myTableName));
        Scan scan = new Scan();
        ResultScanner scanResult = table.getScanner(scan);
        System.out.println("ROW\tCOLUMN+CELL");

        for (Result result : scanResult) {
            String row = new String(result.getRow());
            List<Cell> cells = result.listCells();
            for (Cell c:cells) {
                System.out.println(row+"\t"+new String(CellUtil.cloneFamily(c))+":"+
                        new String(CellUtil.cloneQualifier(c))+", value="+new String(CellUtil.cloneValue(c)));
            }
        }
    }

    public void scanTableByColumn(String myTableName, String colFamily, String col) throws IOException {
        Table table=connection.getTable(TableName.valueOf(myTableName));
        ResultScanner scanResult = table.getScanner(colFamily.getBytes(), col.getBytes());
        System.out.println("ROW\tCOLUMN+CELL");
        for (Result result : scanResult) {
            String row = new String(result.getRow());
            List<Cell> cells = result.listCells();
            for (Cell c:cells) {
                System.out.println(row+"\t"+new String(CellUtil.cloneFamily(c))+":"+
                        new String(CellUtil.cloneQualifier(c))+", value="+new String(CellUtil.cloneValue(c)));
            }
        }
    }

    public void addFamily(String myTableName, String colFamily) throws IOException {
        HTableDescriptor tableDescriptor =  admin.getTableDescriptor(TableName.valueOf(myTableName));
        HColumnDescriptor nColumnDescriptor = new HColumnDescriptor(colFamily);
        tableDescriptor.addFamily(nColumnDescriptor);
        admin.modifyTable(TableName.valueOf(myTableName), tableDescriptor);
        System.out.println("Add column family: "+colFamily+" successfully!");
    }


    public void insertData(String myTableName, String rowKey, String colFamily, String col, String val) throws IOException {
        Table table = connection.getTable(TableName.valueOf(myTableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(colFamily.getBytes(),col.getBytes(),val.getBytes());
        table.put(put);
        table.close();
    }

    public void deleteByCell(String myTableName, String rowKey, String colFamily, String col) throws IOException {
        Table table = connection.getTable(TableName.valueOf(myTableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumns(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        table.delete(delete);
    }

    public void dropTable(String myTableName) throws IOException {
        if (admin.tableExists(TableName.valueOf(myTableName))) {
            admin.disableTable(TableName.valueOf(myTableName));
            admin.deleteTable(TableName.valueOf(myTableName));
            System.out.println("Drop table "+myTableName+" successfully!");
        } else {
            System.out.println("There is no table "+myTableName);
        }
    }

    public static void main (String [] args) throws IOException{
        String myTableName = "StuInfo";
        String[] families= new String[] {"s_info", "c_1", "c_2", "c_3"};

        //建表
        StuInfo operation=new StuInfo();
        operation.init();
        operation.createTable(myTableName, families);

        //插入学生信息
        operation.insertData(myTableName,"2015001","s_info","S_Name", "Li Lei");
        operation.insertData(myTableName,"2015001","s_info","S_Sex", "male");
        operation.insertData(myTableName,"2015001","s_info","S_Age", "23");
        operation.insertData(myTableName,"2015002","s_info","S_Name", "Han Meimei");
        operation.insertData(myTableName,"2015002","s_info","S_Sex", "female");
        operation.insertData(myTableName,"2015002","s_info","S_Age", "22");
        operation.insertData(myTableName,"2015003","s_info","S_Name", "Li Lei");
        operation.insertData(myTableName,"2015003","s_info","S_Sex", "male");
        operation.insertData(myTableName,"2015003","s_info","S_Age", "24");

        //插入学生选课信息
        operation.insertData(myTableName,"2015001","c_1","C_No", "123001");
        operation.insertData(myTableName,"2015001","c_1","C_Name", "Math");
        operation.insertData(myTableName,"2015001","c_1","C_Credit", "2.0");
        operation.insertData(myTableName,"2015001","c_1","C_Score", "86");
        operation.insertData(myTableName,"2015001","c_3","C_No", "123003");
        operation.insertData(myTableName,"2015001","c_3","C_Name", "English");
        operation.insertData(myTableName,"2015001","c_3","C_Credit", "3.0");
        operation.insertData(myTableName,"2015001","c_3","C_Score", "69");
        operation.insertData(myTableName,"2015002","c_2","C_No", "123002");
        operation.insertData(myTableName,"2015002","c_2","C_Name", "Computer Science");
        operation.insertData(myTableName,"2015002","c_2","C_Credit", "5.0");
        operation.insertData(myTableName,"2015002","c_2","C_Score", "77");
        operation.insertData(myTableName,"2015002","c_3","C_No", "123003");
        operation.insertData(myTableName,"2015002","c_3","C_Name", "English");
        operation.insertData(myTableName,"2015002","c_3","C_Credit", "3.0");
        operation.insertData(myTableName,"2015002","c_3","C_Score", "99");
        operation.insertData(myTableName,"2015003","c_1","C_No", "123001");
        operation.insertData(myTableName,"2015003","c_1","C_Name", "Math");
        operation.insertData(myTableName,"2015003","c_1","C_Credit", "2.0");
        operation.insertData(myTableName,"2015003","c_1","C_Score", "98");
        operation.insertData(myTableName,"2015003","c_2","C_No", "123002");
        operation.insertData(myTableName,"2015003","c_2","C_Name", "Computer Science");
        operation.insertData(myTableName,"2015003","c_2","C_Credit", "5.0");
        operation.insertData(myTableName,"2015003","c_2","C_Score", "95");

        //查询选修Computer Science的学生的成绩
        operation.scanTableByColumn(myTableName, "c_2", "C_Score");

        //增加新的列族和新列Contact:Email，并添加数据
        operation.addFamily(myTableName, "Contact");
        operation.insertData(myTableName, "2015001", "Contact", "Email", "lilei@qq.com");
        operation.insertData(myTableName, "2015002", "Contact", "Email", "hmm@qq.com");
        operation.insertData(myTableName, "2015003", "Contact", "Email", "zs@qq.com");

        //删除学号为2015003的学生的选课记录
        operation.deleteByCell(myTableName, "2015003","c_1", "C_No");
        operation.deleteByCell(myTableName, "2015003","c_1", "C_Name");
        operation.deleteByCell(myTableName, "2015003","c_1", "C_Credit");
        operation.deleteByCell(myTableName, "2015003","c_1", "C_Score");
        operation.deleteByCell(myTableName, "2015003","c_2", "C_No");
        operation.deleteByCell(myTableName, "2015003","c_2", "C_Name");
        operation.deleteByCell(myTableName, "2015003","c_2", "C_Credit");
        operation.deleteByCell(myTableName, "2015003","c_2", "C_Score");
        operation.deleteByCell(myTableName, "2015003","c_3", "C_No");
        operation.deleteByCell(myTableName, "2015003","c_3", "C_Name");
        operation.deleteByCell(myTableName, "2015003","c_3", "C_Credit");
        operation.deleteByCell(myTableName, "2015003","c_3", "C_Score");

        //删除所创建的表
        operation.dropTable(myTableName);

        operation.close();
    }
}