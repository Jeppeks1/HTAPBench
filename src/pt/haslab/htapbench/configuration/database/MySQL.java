package pt.haslab.htapbench.configuration.database;

import pt.haslab.htapbench.benchmark.BenchmarkModule;
import pt.haslab.htapbench.configuration.Configuration;

public class MySQL extends Configuration {

    public MySQL(BenchmarkModule benchmarkModule){
        super(benchmarkModule);
    }

    public boolean getTxnControl() { return true; }

    public String getCreate(){
        return String.format("create database if not exists %s", dbname);
    }

    public String getDrop(){
        return String.format("drop database if exists %s", dbname);
    }

    public String getBulk(){
        return "load data local infile '%s' into table %s fields terminated by ',' lines terminated by '\\n'";
    }

    public String getUse(){
        return String.format("use %s", dbname);
    }

    public String getNull(){
        return "\\N";
    }
}
