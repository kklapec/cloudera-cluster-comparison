'''
Copyright 2016 Krzysztof Klapecki, Tomasz Uziemblo

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''

from cm_api.api_client import ApiResource
import urllib2, os, json, time, getpass, csv, argparse, sys, pymysql, imp
from string import replace
pymysql.install_as_MySQLdb()

#CONFIG = os.path.join('c:\\', 'test', 'config.ini')
CONFIG = os.path.join('c:\\', 'Miniconda2',  'config.ini')

params_blacklist = [
    'default', 
    'description', 
    'validationState', 
    'validationMessage',
    'validationWarningsSuppressed', 
    'required'
    ]  
matching_mapping = {'impala1':'impala', 'solr1':'solr' , 'hbase1':'hbase', 
                    'IMPALA1':'impala', 'SOLR1':'solr' , 'HBASE1':'hbase'} 

def list_services():
    print (
        "\nservices and their parameters available on cluster \"{0}\"").format(
            cluster_name
            )
    for service in clu.get_all_services():
        print ("\n"+"-" * 120)    
        print ("{: <15}{: <15}{: <15}").format(
            service.type, 
            service.serviceState, 
            service.healthSummary
            )
        print ("\n{: <30}{: <20}").format('KEY', 'VALUE')
        for k, v in sorted(vars(service).items()):
            if not k.startswith('_'):
                print ("{: <30}{: <20}".format(k, str(v)))
            
def hosts_to_dict():
    global hosts_dict
    hosts_dict = {}
    for host in api.get_all_hosts():
        hosts_dict[host.hostId] = host.hostname     
        
def dump_services_config():
    start = time.time()
    """Crawls through all services and dump their settings to csv file and mySQL database"""
    csv_file = open('{}.csv'.format(cluster_name_with_timestamp), 'wb')
    writer = csv.writer(csv_file)
    writer.writerow(['PARAMETER','KEY','VALUE','FQDN','SERVICE','ROLE'])
    print ("Dumping all services and roles to csv file...")
    for service in clu.get_all_services():          
        print ("Dumping {0} ...").format(service.type)
        for x in service.get_config(view='full'): 
            for _, y in sorted(x.items()):      
                for key, value in sorted(vars(y).items()):
                    if not key.startswith('_') and key not in params_blacklist:
                        try:
                            writer.writerow([
                                y.name, 
                                key, 
                                replace(
                                    repr(str(value).encode('utf-8')),"'",""
                                    ),
                                "", 
                                service.type,
                                ""
                                ])
                        except:
                            print (
                                "Error writing to CSV file with data: "\
                                "{}, {}, {}, {}, {}, {}".format(
                                    y.name,
                                    key,
                                    replace(repr(
                                        str(value).encode('utf-8')),"'",""
                                        ),
                                    "",
                                    service.type,
                                    "" 
                                    ))
        for role in service.get_all_roles():
            for _,y in sorted(role.get_config(view='full').items()): 
                for key, value in sorted(vars(y).items()):
                    if not key.startswith('_') and key not in params_blacklist:
                        try:
                            writer.writerow([
                                y.name,
                                key,
                                replace(
                                    repr(str(value).encode('utf-8')),"'",""
                                    ),
                                hosts_dict[role.hostRef.hostId],
                                service.type, 
                                role.type
                                ])
                        except:
                            print ("Error writing to CSV file with data: "\
                                "{}, {}, {}, {}, {}, {}".format(
                                    y.name, 
                                    key, 
                                    replace(
                                        repr(str(value).encode('utf-8')),"'",""
                                        ),
                                    hosts_dict[role.hostRef.hostId],
                                    service.type, 
                                    role.type 
                                    ))
                           
    mgmt = api.get_cloudera_manager().get_service() 
    for role in mgmt.get_all_roles():
        print ("Dumping {0} ...".format(role.type))
        for _,y in sorted(role.get_config(view='full').items()):
            for key, value in sorted(vars(y).items()):
                if not key.startswith('_') and key not in params_blacklist: 
                    try:
                        writer.writerow([
                            y.name,
                            key, 
                            replace(repr(str(value).encode('utf-8')),"'",""), 
                            hosts_dict[role.hostRef.hostId], 
                            mgmt.type, 
                            role.type
                            ])
                    except:
                        print ("Error writing to CSV file with data: "\
                            "{}, {}, {}, {}, {}, {}".format(
                                y.name, 
                                key, 
                                replace(
                                    repr(str(value).encode('utf-8')),"'",""
                                    ), 
                                hosts_dict[role.hostRef.hostId], 
                                mgmt.type, 
                                role.type
                                ))
                    
    csv_file.close()
    print ("Dumping completed. File \"'{0}.csv' \"has been created "\
        "in your current dir.".format(
            cluster_name_with_timestamp
            ))
    print ("Execution time: {0} seconds.".format(int(time.time() - start)))
    if mysql_dump is True:
        csv_to_mysql()

def csv_to_mysql():
    """Importing data from temporary local csv file to MySQL database"""
    try:
        cfg = imp.load_source('cfg', CONFIG)
    except Exception:
        print ("ERROR: Unable to load config file, "\
            "please check path to config file in python script")
        sys.exit(1)
    loading_start = time.time()
    dbhost = cfg.dbhost
    dbuser = cfg.dbuser
    dbpasswd = cfg.dbpasswd
    db = cfg.db
    dbtable = cluster_name_with_timestamp # do not use config.ini
    print ("Establishing connection with MySQL using DBHOST={}, DB={}, "\
        "TABLE={}. (If you wish to change it, please edit config.ini)".format(
            dbhost, 
            db, 
            dbtable
            ))
    conn = pymysql.connect(
        host=dbhost, 
        user=dbuser, 
        passwd=dbpasswd, 
        charset='utf8', 
        local_infile=True,
        autocommit=True
        ) 
    cur = conn.cursor()
    db='cloudera_comparisons'
    cur.execute(("CREATE DATABASE IF NOT EXISTS {}").format(
        db
        ))
    cur.execute(("USE {}").format(
        db
        ))
    # create table if doesnt exists  SHOW VARIABLES LIKE '%LOCAL%';
    try:
        exists = cur.execute((
            """
            SELECT TABLE_NAME 
            FROM information_schema.tables 
            WHERE table_schema = '{0}' 
            AND table_name = '{1}'""".format(
                db, 
                dbtable
                )))
        if not exists:
            print (
                "Creating new table \'{}\' in database \'{}\'").format(
                dbtable,
                db)
            cur.execute(("""
                CREATE TABLE IF NOT EXISTS {0} (cluster VARCHAR(50) 
                NOT NULL, timestamp DATETIME 
                NOT NULL, parameter VARCHAR(255) 
                NOT NULL, keyy VARCHAR(255) 
                NOT NULL, value TEXT 
                NOT NULL, fqdn VARCHAR(100), service VARCHAR(100), 
                role VARCHAR(100))""".format(
                    dbtable
                    )))
#            cur.execute(("""
#                ALTER TABLE {}
#                ADD UNIQUE unique_columns (
#                cluster, 
#                timestamp, 
#                parameter, 
#                keyy, 
#                value(30), 
#                fqdn, 
#                service, 
#                role)""".format(
#                    dbtable
#                    )))
    except pymysql.Error, e:
        print ("SQL Error {}: {}".format(e.args[0],e.args[1]))
    # creating temporary csv file
    csv_handler   = csv.reader(open(
        '{}.csv'.format(cluster_name_with_timestamp), 
        'rb'
        ))
    outfile = open('db_load_temp.csv', 'wb')
    for line in csv_handler:
        if csv_handler.line_num == 1:   
            pass # do not add header   csv_table.append(line) #writer_summary.writerow([ line[0], line[1], line[2], line[3], line[4] ])
        else:
            outfile.write('{},{},{},{},\t{}\t,{},{},{},{}'.format(
                cluster_name, 
                now, 
                line[0], 
                line[1], 
                line[2], 
                line[3], 
                line[4], 
                line[5], 
                '\n'
                ))
    outfile.close()
    # dumping csv to database    
    print ("Dumping data to MySQL database...")
    try:                                
        cur.execute("LOAD DATA LOCAL INFILE 'db_load_temp.csv' INTO TABLE {0} "\
            "FIELDS TERMINATED BY ',' ENCLOSED BY '\t' "\
            "LINES TERMINATED BY '\n'".format(dbtable))
        # cur.execute("INSERT INTO %s (cluster, timestamp, parameter, keyy, value, fqdn, service, role) VALUES ('%s', '%s', '%s','%s','%s','%s','%s','%s');" % (dbtable, cluster_name, now, line[0], line[1], line[2], line[3], line[4], line[5]))
    except pymysql.Error, e:
        print ("SQL Error %d: %s" % (e.args[0],e.args[1]))
    finally:
        os.remove('db_load_temp.csv')
        if conn:
            conn.close()
    print ("Execution time: {} seconds.".format(
        int(time.time() - loading_start)
        ))

def login():
    p = urllib2.HTTPPasswordMgrWithDefaultRealm()
    p.add_password(None, 'http://{0}:{1}/api/v10/clusters'.format(
        server_name, 
        port, 
        username, 
        password
        ))
    handler = urllib2.HTTPBasicAuthHandler(p)
    opener = urllib2.build_opener(handler)
    urllib2.install_opener(opener)

def download_file(url, plik, role):
    if '/' in plik:
        dirPath  = os.path.join(
            os.path.abspath(os.curdir), 
            cluster_name_with_timestamp, 
            role, plik.split('/')[0]
            )
        filePath = os.path.join(
            os.path.abspath(os.curdir), 
            cluster_name_with_timestamp, 
            role, 
            plik.split('/')[0], 
            plik.split('/')[1]
            )
    else:
        dirPath  = os.path.join(
            os.path.abspath(os.curdir), 
            cluster_name_with_timestamp, 
            role
            )
        filePath = os.path.join(
            os.path.abspath(os.curdir), 
            cluster_name_with_timestamp, 
            role, 
            plik.split('/')[0]
            )
    
    if plik.count('/') == 2:  # here it should be file name, if it's another folder, create it
        dirPath  = os.path.join(
            os.path.abspath(os.curdir), 
            cluster_name_with_timestamp, 
            role, 
            plik.split('/')[0], 
            plik.split('/')[1]
            )
        filePath = os.path.join(
            os.path.abspath(os.curdir), 
            cluster_name_with_timestamp, 
            role, 
            plik.split('/')[0], 
            plik.split('/')[1], 
            plik.split('/')[2]
            )
    if not os.path.exists(dirPath):
        try:
            os.makedirs(dirPath)
        except OSError, e:
            print e
    try:
        f = urllib2.urlopen(url)       
        with open(filePath, "wb") as local_file:
            local_file.write(f.read()) ### .encode('utf-8')
            local_file.close()
    except urllib2.HTTPError, e:
        print ("HTTP Error:", e.code, url)
    except urllib2.URLError, e:
        print ("URL Error:", e.servicesreason, url)
    except IOError, e:
        print ("IO Error:", e, url)

def extract_info():    #uses WEB REST to dump config files
    cluster_name_no_spaces = cluster_name.replace(' ', '%20', 10)
    for service in clu.get_all_services():
        print ("Dumping {0}...".format(service.type))
        for role in sorted(service.get_all_roles()):
            url_processes = 'http://{0}:{1}/api/v10/clusters/{2}/services/{3}/"\
            "roles/{4}/process/'.format(
                server_name, 
                port, 
                cluster_name_no_spaces, 
                service.name, 
                role.name
                )
            try:
                json1_data = json.loads(urllib2.urlopen(url_processes).read()) 
                for plik in json1_data['configFiles']:
                    url = '{0}configFiles/{1}'.format(url_processes, plik)   
                    download_file(url, plik, role.name)
            except urllib2.HTTPError:
                pass #print "Error: ", e
            except KeyError:
                pass #print "Error: ", e
            except:
                pass

def compare_files():   # only with '_summary' in filename
    """Comparing 2 csv files with configurations (_summary)"""
#==============================================================================
# method 1                               # method 2
# glob.glob('*.csv')[3]                  # [f for f in os.listdir('.') if re.match(r'.*\.csv', f)]
#==============================================================================
    while True:
        available_csv_files = [fn for fn in os.listdir('.') if 
        any(fn.endswith(ext) for ext in ['summary.csv'])]
        print ("\nAvailable CSV files in current folder:\n")
        for index, name in enumerate(available_csv_files):
            print ('{0}-->{1}'.format(index+1, name))
        c1 = raw_input("Choose 1st file to compare " \
            "(must be with impala1, hbase1 services): ")
        c2 = raw_input("Choose 2nd file to compare " \
            "(must be with impala, hbase services): ")
        try:
            file1 = available_csv_files[int(c1)-1]
            file2 = available_csv_files[int(c2)-1]
            if c1==c2:
                raise Exception
            break
        except:
            print ("\n\nTry again.")
    csv_file = open(
        '{0}_vs_{1}.csv'.format(
            file1.replace('_summary.csv', ''), 
            file2.replace('_summary.csv', '')), 
        'wb'
        )
    writer = csv.writer(csv_file)
    writer.writerow([
        'PARAMETER',
        'KEY', 
        'SERVICE',
        'ROLE', 
        'VALUE in {0} VALUE in {1}'.format(file1, file2)
        ])
    csv1 = csv.reader(open(file1, 'rb'))
    csv2 = csv.reader(open(file2, 'rb'))
    csv1_table = []
    csv2_table = []
    set1 = set()
    set2 = set()
    lines_in_1_not_found_in_2 = []
    for line in csv1:
        set1.add(line[3].lower() if not matching_mapping.has_key(line[3]) else 
            matching_mapping[line[3]])
        if line[1] in params_blacklist:
            continue
        else:
            if matching_mapping.has_key(line[3]): # replace names according to dictionary
                line[4] = line[4].replace(line[3], matching_mapping[line[3]])
                line[3] = matching_mapping[line[3]]
            csv1_table.append(line)
    for line in csv2:
        set2.add(line[3].lower() if not matching_mapping.has_key(line[3]) else 
            matching_mapping[line[3]])
        if line[1] in params_blacklist:
            continue
        else:
            if matching_mapping.has_key(line[3]): # replace names according to dictionary
                line[4] = line[4].replace(line[3], matching_mapping[line[3]])
                line[3] = matching_mapping[line[3]]
            csv2_table.append(line)
    
    csv1_table_sorted = sorted(csv1_table)
    csv2_table_sorted = sorted(csv2_table)
    print ("\nComparing \"{0}\" against \"{1}\". Value differences will be "\
        "shown below:\n".format(file1, file1))
    for line1 in csv1_table_sorted:
        csv2_table_sorted = csv2_table_sorted # reset seeking from 0 row
        for line2 in csv2_table_sorted:
            if line1[0]+line1[1]+line1[3]+line1[4] == \
            line2[0]+line2[1]+line2[3]+line2[4]:  
                if line1[2] != line2[2]:
                    print ('{0},{1},{2},{3},\tcsv1: {4} \tcsv2: {5}'.format(
                        line1[0], 
                        line1[1], 
                        line1[3], 
                        line1[4], 
                        line1[2], 
                        line2[2]
                        ))
                    writer.writerow([
                        line1[0], 
                        line1[1], 
                        line1[3], 
                        line1[4], 
                        line1[2], 
                        line2[2] 
                        ])
                    csv2_table_sorted.remove(line2)
                    break
                else:
                    csv2_table_sorted.remove(line2)
                    break
        else:
            lines_in_1_not_found_in_2.append(line1) #print "This line in", "\""+file1+"\"", "does not exist in" , "\""+file2+"\"\n", ','.join(line1)
    print len(lines_in_1_not_found_in_2)
    if len(set1.difference(set2))>0 or len(set2.difference(set1))>0:
        print ("\nServices available in {} but not in {}: {}".format(
            file1, 
            file2, 
            set1.difference(set2)
            ))
        print ("Services available in {} but not in {}: {}".format(
            file2, 
            file1, 
            set2.difference(set1)
            ))
        writer.writerow(["\nServices available in {} but not in {}: {}".format(
            file1,
            file2, 
            set1.difference(set2)
            )])
        writer.writerow(["Services available in {} but not in {}: {}".format(
            file2, 
            file1, 
            set2.difference(set1)
            )])
 #==============================================================================
# uncomment this if you want to see all parameters line by line
#         print "\n\nComparing completed, but", len(csv2_table_sorted) , "rows (configuration parameters) in each file exist in one file and not in another and vice versa, so cannot be compared."
#         print "\nConfiguration parameters in \"" + file1 + "\" " + "but not avilable in \"" + file2 + "\":"
#         for x in lines_in_1_not_found_in_2:
#             print ','.join(x)
#         print "\nConfiguration parameters in \"" + file2 + "\" " + "but not avilable in \"" + file1 + "\":"
#         for x in csv2_table_sorted:
#             print ','.join(x)
#==============================================================================
    print ("\n\nCompare completed. See {0}_vs_{1}.csv".format(
        file1.replace('_summary.csv', ''), 
        file2.replace('_summary.csv', '')
        ))
    csv_file.close()
    
def compare_and_filter_redundant_nodes_entries():
    """Compares nodes of same cluster between themselves, cluster self-comparing"""
    print ("Comparing node vs node in cluster...")
    csv_content = csv.reader(open(
        '{}.csv'.format(cluster_name_with_timestamp), 
        'rb'
        )) 
    output_handler = open(
        '{}_summary.csv'.format(cluster_name_with_timestamp), 
        'wb'
        )
    output_handler_same_cluster = open(
        '{}_self_diff.csv'.format(cluster_name_with_timestamp), 
        'wb'
        )
    writer_summary = csv.writer(output_handler)
    writer_same_cluster = csv.writer(output_handler_same_cluster)
    csv_table = []
    for line in csv_content:
        if csv_content.line_num == 1:  
            pass # do not add header   csv_table.append(line) #writer_summary.writerow([ line[0], line[1], line[2], line[3], line[4] ])
        else:
            csv_table.append(line)
    csv_table_sorted = sorted(csv_table)
    temporary_dict = {}
    differences_set = set()
    unique_keys = set()
    current_key = ''
    writer_summary.writerow([
        'PARAMETER',
        'KEY',
        'VALUE',
        'SERVICE',
        'ROLE'
        ]) #csv header                               
    writer_same_cluster.writerow([
        'PARAMETER',
        'KEY',
        'VALUE',
        'FQDN',
        'SERVICE',
        'ROLE'
        ]) #csv header
    for line in csv_table_sorted:
        if line[1] in params_blacklist:
            continue
        if line[5] == '':   # situation where there is no parameter, just service name, we can directly write to summary,no need to compare
            writer_summary.writerow([
                line[0], 
                line[1], 
                line[2], 
                line[4], 
                line[5] 
                ])  #summary without FQDN
        else:
            key = '{0},{1},{2},{3}'.format(
                line[0], 
                line[1], 
                line[4], 
                (line[5][:-33] if line[5].count('-') > 1 else line[5])
                )
            if current_key == key:
                # another line same service (e.g. DATANODE)
                if key in temporary_dict:
                    if temporary_dict[ key ] != line[2]:
                        #print "\nDifference found in: ", line[0]+','+line[1]+','+line[3]+','+line[4], '\nValue in one node:', temporary_dict[ key ], '\nValue in another :', line[2]
                        differences_set.add(key)
                        temporary_dict[ key ] = "*****Difference found under "\
                        "this key*****"
                else: # first-time value
                    temporary_dict[ key ] = line[2]
                    if key not in unique_keys:
                        writer_summary.writerow([
                            line[0], 
                            line[1], 
                            line[2], 
                            line[4], 
                            (line[5][:-33] if line[5].count('-') > 1 else 
                                line[5]) 
                            ])
                        unique_keys.add(key)
            else:  # first row with new service
                current_key = key
                temporary_dict = {}
                temporary_dict[ key ] = line[2]
                if key not in unique_keys:
                    writer_summary.writerow([
                        line[0], 
                        line[1], 
                        line[2], 
                        line[4], 
                        (line[5][:-33] if line[5].count('-') > 1 else line[5])
                        ])
                    unique_keys.add(key)
    
    for line in csv_table_sorted:       # writing to _self_diff file all rows with at least one difference
        if '{0},{1},{2},{3}'.format(
                line[0], 
                line[1], 
                line[4], 
                (line[5][:-33] if line[5].count('-') > 1 else line[5]) in 
                differences_set):
            writer_same_cluster.writerow([
                line[0],
                line[1],
                line[2],
                line[3],
                line[4], 
                line[5]
                ])
    
    output_handler.close()
    output_handler_same_cluster.close()
    if not differences_set:
        print ("Compare completed, nodes in cluster are config-consistent")
    else:
        print ("Compare completed, differences found. Files below were " \
            "created in current folder:")
        print ('{0}.csv            --> contains full dump'.format(
            cluster_name_with_timestamp
            ))
        print ('{0}_summary.com    --> contains flattened dump, " \
            "see note below'.format(cluster_name_with_timestamp))
        print ('{0}_self_diff.csv  --> contains node vs node differences " \
            "of {1}'.format(cluster_name_with_timestamp, cluster_name))
        print ("\nPlease note that if there are differences in node " \
            "configuration, only one value")
        print ("will be written to \"*_summary.csv\" which will be used to " \
            "compare against other cluster's configuration.")
        print ("\nIf node's settings in single cluster/environment are "\
            "not synchronized")
        print ("it will lead to improper comparison against another cluster")
        print ("Script has finished.")

def gather_details(args, cfg, SECOND_HOST=False):
    global cluster_name, clu, cluster_name_with_timestamp, api, server_name, port, username, password, now, mysql_dump
    if cfg:
        try: 
            mysql_dump = cfg.mysql_dump
            server_name = cfg.server_name
            port = cfg.port
            cluster_name = cfg.cluster_name
            username = cfg.username
        except Exception:
            print ("\nUnable to properly load parameters from {}. They must "\
                "have been commented out.".format(CONFIG))
            print ("Parameters for script will be taken *only* from command "\
                "line. Config.ini to be ignored completely.")
            cfg = False
    
    if args.mysql:
        mysql_dump = args.mysql
        
    if args.server_name:
        server_name = args.server_name
    elif cfg:
        pass #already assigned at the beginning of function
    else:
        print ("ERROR: No server hostname provided. Please provide via "\
            "config.ini or command line. Script will quit now.")
        sys.exit(1)
        
    if args.port:
        port = args.port
    elif cfg:
        pass #already assigned at the beginning of function
    else:
        print ("ERROR: No port number provided. Please provide via " \
            "config.ini or command line. Script will quit now.")
        sys.exit(1)
        
    if args.username:
        username = args.username
    elif cfg:
        pass #already assigned at the beginning of function
    else:
        print ("ERROR: No username provided. Please provide via config.ini or"\
            " command line. Script will quit now.")
        sys.exit(1)
        
    if args.cluster_name:
        cluster_name = args.cluster_name
    elif cfg:
        pass #already assigned at the beginning of function
    else:
        print ("ERROR: No cluster name provided. Please provide via " \
            "config.ini or command line. Script will quit now.")
        sys.exit(1)
    # processing second cluster, only host, port and name. Login/pass, DB details remain the same
    if SECOND_HOST and cfg:
        try: 
            server_name = cfg.server_name2
            port = cfg.port2
            cluster_name = cfg.cluster_name2
        except Exception:
            print ("ERROR: Unable to properly load SECOND cluster parameters" \
                " from {}. They must have been commented out".format(CONFIG))
            print ("ERROR: Please provide via config.ini. " \
                "Script will quit now.")
            sys.exit(1)
        
    print ("Server\t\t:{}".format(server_name))
    print ("Cluster\t\t:{}".format(cluster_name))
    print ("Username\t:{}".format(username))
    print ("MySQL dump\t:{}".format(mysql_dump))
    if 'password' in locals() or 'password' in globals():  # this is for SECOND host, if password has already been typed, will not be again
       pass
    else:
        if not args.password:
            try:
                password = cfg.password
            except:
                password = str(getpass.getpass("Type password\t:"))
        else:
            password = args.password
    
    try:
        api = ApiResource(
            server_name, 
            port, 
            username=username, 
            password=password, 
            version='10'
            )

        count_clusters = []
        for c in api.get_all_clusters():
            count_clusters.append(c.displayName)             #print "Available clusters: %d, %s, %s" % (len(count_clusters)+1, c.displayName, c.name)   # or   displayName !!!!
        if not cluster_name in count_clusters: #if cluster user provided does not exist
            if len(count_clusters) > 1:
                print ("Available clusters:" + '\n--> '.join(count_clusters))
                cluster_name = raw_input("Type cluster name without quotes: ")
            else:
                cluster_name = c.displayName
                print (("Cluster you typed is not valid. " \
                    "Using cluster: \"{}\"".format(cluster_name)))
        clu = api.get_cluster(cluster_name)
    except urllib2.URLError:
        print ("ERROR: Can't connect to host, make sure you are connected " \
            "to VPN and hostname is valid")
        sys.exit(1)
    except:
        print ("ERROR: Wrong password. Script will now exit.")
        sys.exit(1)
    else:
        print ("Connected successfully")

    now = time.strftime('%Y%m%d%H%M%S')
    cluster_name_with_timestamp = '{}_{}'.format(
        cluster_name.replace(' ', '_', 10), 
        now
        )
    hosts_to_dict()

def main():
    parser = argparse.ArgumentParser(description="Exports and compare " \
        "Cloudera Clusters configuration. Command line takes precedence " \
        "over config.ini")
    parser.add_argument("-s", "--server-name", help="host name or IP, with " \
        "no quotes, e.g. 10.10.10.10")
    parser.add_argument("--port", help="CM port, e.g. 7180", default=7180)
    parser.add_argument("-c", "--cluster-name", help="cluster name")
    parser.add_argument("-u", "--username", help="Username with admin " \
        "privileges")
    parser.add_argument("-p", "--password")
    parser.add_argument("-a", "--action", help="run script with no params " \
        "to see options", type=int, choices=[1, 2, 3, 4])
    parser.add_argument("--mysql", help="appends dump to MySQL database")
    parser.add_argument("--second", action='store_true', help="use details " \
        "of second hostname (only via config.ini) to perform two dumps " \
        "in one go. Flag set = True, no flag = False.")
    args = parser.parse_args()
    try:  #reading just action parameter as some of script options can be executed offline
        cfg = imp.load_source('cfg', CONFIG)
        selection = int(cfg.action)
    except Exception:
        print ("\nUnable to properly load {} file. File was removed or its " \
            "variables have been commented out.".format(CONFIG))
        print ("Parameters for script will be taken only from command line.")
        cfg = False
    
    if args.action:
        selection = int(args.action)
    elif cfg == False:
        try:
            selection = int(args.action)
        except:
            print ("ERROR: Cannot load value for action. Please use command " \
                "line parameter \'-a\' to provide value")
            sys.exit(1)
    
    print ("Selected action :{}".format(selection))
    if selection == 1:    # API based
        gather_details(args, cfg)
        list_services()
        if args.second:
            print ("Dumping second cluster...")
            SECOND_HOST = True
            gather_details(args, cfg, SECOND_HOST)
            list_services()
    elif selection == 2:  # API based
        gather_details(args, cfg)
        dump_services_config()
        compare_and_filter_redundant_nodes_entries()
        if args.second:
            print ("Dumping second cluster...")
            SECOND_HOST = True
            gather_details(args, cfg, SECOND_HOST)
            dump_services_config()
            compare_and_filter_redundant_nodes_entries()
    elif selection == 3:  # API + REST based
        gather_details(args, cfg)
        login()
        extract_info()
        if args.second:
            print ("Dumpint second cluster...")
            SECOND_HOST = True
            gather_details(args, cfg, SECOND_HOST)
            extract_info()
    elif selection == 4:  # No internet/VPN required
        compare_files()
    else:
        print ("Goodbye")

if __name__ == '__main__':
    main()