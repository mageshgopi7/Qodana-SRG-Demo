# Get The History Load Extract Parameter Scalar Variables

gv_Activeflag=activeflag
gv_Tablename=tablename
gv_Startdate=startdate
gv_Enddate=enddate
gv_Partitioncolumn=partitioncolumn
gv_Exculdelist=exculdelist
gv_Delimiter=delimiter
gv_Filesize=filesize
gv_Source=source
gv_LoadType=loadtype
gv_Database=database
gv_Sfstage=sfstage
gv_Sfstagedb=sfstagedb
gv_Sfstageschema=sfstageschema

def History_Snowflake_Gen(gv_Activeflag,gv_Tablename,gv_Startdate,gv_Enddate,gv_Partitioncolumn,gv_Exculdelist,gv_Delimiter,gv_Filesize,gv_Sfstage,gv_Source,gv_LoadType,gv_Database):
    """ Return the Final Copy Stage to S3 Bucket SQL Statement

    Arguments:
    SQL_History: Contains Snowflake Copy Stage Sql Script
    """
    ActiveFlag=gv_Activeflag
    Tablename=gv_Tablename
    Startdate=gv_Startdate
    Enddate=gv_Enddate
    Partitioncolumn=gv_Partitioncolumn
    Exculdelist=gv_Exculdelist
    Delimiter=gv_Delimiter
    Filesize=gv_Filesize
    Sfstage=gv_Sfstage
    Source=gv_Source
    LoadType=gv_LoadType
    Database=gv_Database
    FOE='"'

    SQL_History=''
    if ActiveFlag=='Y' and LoadType=='DELTA':

        TableDetail=str(Database)+"."+str(Source)+"."+str(Tablename)+"_0"
        FileName = "DL_"+str(Source)+"_"+str(Tablename)+"_"+str(LoadType)+"_"+str(Startdate)+"_"+str(Enddate)+"_history"
        Historysql= "(SELECT * EXCLUDE ("+str(Exculdelist)+") FROM "+str(TableDetail)+" where "+str(Partitioncolumn)+" >="+str(Startdate)+" and "+str(Partitioncolumn)+" <= "+str(Enddate)+")"
        SFParams="file_format = (TYPE = CSV FIELD_DELIMITER ='"+str(Delimiter)+"' compression='gzip' FIELD_OPTIONALLY_ENCLOSED_BY ='"+str(FOE)+"'"+"  ESCAPE_UNENCLOSED_FIELD = NONE null_if=('')) header = true SINGLE = FALSE max_file_size="+Filesize+";"
        SQL_History = "COPY INTO @"+str(Sfstage)+"/"+str(FileName)+" FROM "+str(Historysql)+str(SFParams)


    elif ActiveFlag=='Y' and LoadType=='FULL':

        TableDetail=str(Database)+"."+str(Source)+"."+str(Tablename)+"_0"
        FileName = "DL_"+str(Source)+"_"+str(Tablename)+"_"+str(LoadType)+"_"+str(Startdate)+"_"+str(Enddate)+"_history"
        Historysql= "(SELECT * EXCLUDE ("+str(Exculdelist)+") FROM "+str(TableDetail)+")"
        SFParams="file_format = (TYPE = CSV FIELD_DELIMITER ='"+str(Delimiter)+"' compression='gzip' FIELD_OPTIONALLY_ENCLOSED_BY ='"+str(FOE)+"'"+"  ESCAPE_UNENCLOSED_FIELD = NONE null_if=('')) header = true SINGLE = FALSE max_file_size="+Filesize+";"
        SQL_History = "COPY INTO @"+str(Sfstage)+"/"+str(FileName)+" FROM "+str(Historysql)+str(SFParams)


    return SQL_History
    print("Copy To S3 Bucket Sql Generated successfully")

def History_Sql(SQL_History,gv_Sfstagedb,gv_Sfstageschema):
    """ Update the Final COPY Stage SQL Command to the Variable

    Arguments:
    SQL_History: Contains COPY Stage SQL Command
    """
    context.updateVariable('sqlcmd',SQL_History)
    #return SQL_History
    # A database Snowflake cursor can be accessed from the context (Jython only)
    cursor = context.cursor()
    # Define Stage Schema for executing Copy Stage Command
    sfstagedb =gv_Sfstagedb
    sfstageschema=gv_Sfstageschema
    sfstagesql = "USE "+str(sfstagedb)+"."+str(sfstageschema)
    print(sfstagesql)
    cursor.execute(sfstagesql)
    # Execute Copy Stage Command
    print(sqlcmd)
    cursor.execute(sqlcmd)

def main():

    history_gen=History_Snowflake_Gen(gv_Activeflag,gv_Tablename,gv_Startdate,gv_Enddate,gv_Partitioncolumn,gv_Exculdelist,gv_Delimiter,gv_Filesize,gv_Sfstage,gv_Source,gv_LoadType,gv_Database)
    History_Sql(history_gen,gv_Sfstagedb,gv_Sfstageschema)
    print("Copy Stage sqlcmd is:",sqlcmd)
    print("History Load files for "+str(gv_Tablename)+" is completed")

if __name__ == "__main__":
    main()