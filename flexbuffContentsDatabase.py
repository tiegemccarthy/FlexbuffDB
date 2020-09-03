#!/usr/bin/python3

from paramiko import SSHClient
from scp import SCPClient
import MySQLdb as mariadb
import os
import sys

dirname = os.path.dirname(__file__)

def flexbuffContentsPull(flexbuff_tag): # function for scp of disk_used.txt file from a given flexxbuff machine to cwd.
    flexbuff_tag = str(flexbuff_tag)
    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.connect(hostname= flexbuff_tag + '.phys.utas.edu.au', 
                  username='observer',
                  #password=''
                 )

    scp = SCPClient(ssh.get_transport())
    scp.get('/tmp/disk_used.txt', dirname + '/disk_used_' + flexbuff_tag + '.csv')
    scp.close()
    ssh.close()

def updateFlexbuffContents(flexbuff_tag, db_name): # add data from disk_used file into the SQL database.
    flexbuff_tag = str(flexbuff_tag)
    db_name = str(db_name)
    # connect to the database
    conn = mariadb.connect(user='auscope', passwd='password', db=db_name, local_infile = 1)
    cursor = conn.cursor()
    # This process seems somewhat redundant but is best practice for ensuring table data is not deleted before ensuring new data has been loaded without errors.
    # Create a table for the new inbound data named flexbuffXX_NEWSELECT @@GLOBAL.sql_mode;
    query = "CREATE TABLE IF NOT EXISTS "+ flexbuff_tag + "_NEW (ExpID VARCHAR(10) NOT NULL PRIMARY KEY, DataUsage BIGINT, TimeStamp DATETIME);" 
    cursor.execute(query)
    conn.commit()
    # Create a temporary 'current' flexbuffXX table, this table will only not exist on a first run
    query = "CREATE TABLE IF NOT EXISTS "+ flexbuff_tag + " (ExpID VARCHAR(10) NOT NULL PRIMARY KEY, DataUsage BIGINT, TimeStamp DATETIME);" 
    cursor.execute(query)
    conn.commit()
    # Load CSV data into the newly created flexbuffXX_NEW table
    query = "LOAD DATA LOCAL INFILE '" + dirname + "/disk_used_" + flexbuff_tag + ".csv' REPLACE INTO TABLE "+ flexbuff_tag + "_NEW FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' (ExpID, DataUsage, @TimeStamp) SET TimeStamp = STR_TO_DATE(@TimeStamp,'%d %b %Y');"
    cursor.execute(query)
    conn.commit()
    # Drop any existing flexbuffXX_OLD table (this will soon be replaced by the current flexbuffXX table)
    query = "DROP TABLE IF EXISTS " + flexbuff_tag + "_OLD;"
    cursor.execute(query)
    conn.commit()
    # Rename current flexbuffXX table that is getting replaced to flexbuffXX_OLD
    query = "ALTER TABLE " + flexbuff_tag + " RENAME TO " + flexbuff_tag + "_OLD;"
    cursor.execute(query)
    conn.commit()
    # Rename the new data table flexbuffXX_NEW to flexbuffXX
    query = "ALTER TABLE " + flexbuff_tag + "_NEW RENAME TO " + flexbuff_tag + ";"
    cursor.execute(query)
    conn.commit()
    
def main(db_name):
    db_name = str(db_name)
    # create/connect to mariaDB flexbuff database
    print('Connecting to mariaDB database ' + db_name + '.')
    conn = mariadb.connect(user='auscope', passwd='password')
    cursor = conn.cursor()
    query = "CREATE DATABASE IF NOT EXISTS " + db_name +";"
    cursor.execute(query)
    conn.commit()
    # setup the tags for the relevant flexbuff machines
    flexbuff_id = ['flexbuffhb', 'flexbuffke', 'flexbuffyg', 'flexbuffcd']
    # for each flexbuff machine, pull relevant disk usage data and add to the database
    for flexbuff in flexbuff_id:
        print('Pulling disk contents file from ' + flexbuff + '.')
        flexbuffContentsPull(flexbuff)
        print('Adding disk contents to the ' + flexbuff + ' table of the ' + db_name + ' mariaDB database.')
        updateFlexbuffContents(flexbuff, db_name)



if __name__ == '__main__':
    # auscope_file_scraper.py executed as a script
    main(sys.argv[1])
