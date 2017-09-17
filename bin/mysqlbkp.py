#!/usr/bin/python
 
from os import walk
 
import re, time, datetime, configparser, sys, os, subprocess, gzip
 
def print_usage(script):
    print('Usage:', script, '--config <config file>', '--dir <target backup directory>', '--pf <mysql password file>')
    sys.exit(1)

def check_location(file, desc):
    expandfile = os.path.expanduser(file)
    if not os.path.exists(expandfile):
        print('Error:', desc, file, 'was not found')
    sys.exit(1)
    else:
        return expandfile
 
# read configuration file and input parameters
def init_config(args):

    config = {
        'MAIN.BackupDir' : '',
        'MAIN.MySqlUserFile' : '',
        'PURGE.DaysToKeep' : '',
        'BACKUP.DirsToBackup' : ''
    }

    # check if config file was provided and assign it to variable
    if not '--config' in args and not os.access(os.path.expanduser('~/sitebackup/etc/mysqlbkp.cfg'), os.R_OK):
        print('Error: Configuration file was not found') 
    print_usage(args[0])
    else:
        configfile = configparser.SafeConfigParser()
        if not '--config' in args:
            configfile.read(os.path.expanduser('~/sitebackup/etc/mysqlbkp.cfg'))
            config['MAIN.MySqlUserFile'] = check_location(os.path.expanduser('~/sitebackup/etc/mysqlbkp.cfg'), 'Configuration file')
        else:
        configfile.read(check_location(args[args.index('--config')+1], 'Configuration file'))
            config['MAIN.MySqlUserFile'] = check_location(args[args.index('--config')+1], 'Configuration file')
            args.pop(args.index('--config')+1)
            args.pop(args.index('--config'))
    
    # Parsing of comandline parameters
    if '--dir' in args:
        config['MAIN.BackupDir'] = check_location(args[args.index('--dir') + 1], 'Backup directory')
        args.pop(args.index('--dir') + 1)
        args.pop(args.index('--dir'))
    else:
        config['MAIN.BackupDir'] = check_location(configfile.get('MAIN', 'BackupDir'), 'Backup directory')
    
    if len(args) > 1:
        print(args)
        print_usage(args[0]) 
    
    # Populate configuration structure from config file
    config['PURGE.DaysToKeep'] = configfile.get('PURGE', 'DaysToKeep')
    config['BACKUP.DirsToBackup'] = configfile.get('BACKUP', 'DirsToBackup')

    return config
 
# create list of databases which are available for provided mysql user, this user has to have SELECT and LOCK TABLE for this database
def mysql_dblist(cnf):
    no_backup = ['Database', 'information_schema', 'performance_schema', 'test']
    cmd = ['mysql', '--defaults-extra-file='+cnf, '-e', 'show databases']
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    if p.returncode > 0:
        print('MySQL Error:')
        print(stderr)
        sys.exit(1)
    dblist = stdout.strip().split('\n')
    for item in no_backup:
        try:
            dblist.remove(item)
        except ValueError:
            continue

    if len(dblist) == 0:
        print("Doesn't appear to be any databases found")
    sys.exit(1)
     
    return dblist
 
# backup of databases which are available for provided mysql user, this user has to have SELECT and LOCK TABLE for this database
def mysql_backup(config, dblist):
    bresults = {}
    ufile = os.path.expanduser(config['MAIN.MySqlUserFile'])
    for db in dblist:
        bfile = os.path.join(os.path.expanduser(config['MAIN.BackupDir']), db+'_'+datetime.datetime.now().strftime('%Y%m%d%H%M')+'.sql')
        if db == 'mysql':
            cmd = ['mysqldump', '--defaults-extra-file='+ufile, '--max_allowed_packet=512M', '--events', db]
        else:
            cmd = ['mysqldump', '--defaults-extra-file='+ufile, '--max_allowed_packet=512M', '--skip-extended-insert', '--quick', '--single-transaction', db]

        # 3 attempts to backup database, sometimes backup can fail without any good reason(server watchdog), in this case we would like to try it again 
        i = 1
        while i != 3:
            try:
                p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        outp = p.communicate()[0]

        gz = gzip.open(bfile+'.gz','wb')
        gz.write(outp)
        retcode = p.wait()
            except:
        retcode = 255

        gz.close()

            # re-point symlink to the latest database backup
            if os.access(os.path.join(os.path.expanduser(config['MAIN.BackupDir']),'current-'+db+'.sql.gz'), os.F_OK):
                os.unlink(os.path.join(os.path.expanduser(config['MAIN.BackupDir']),'current-'+db+'.sql.gz'))
            os.symlink(bfile+'.gz',os.path.join(os.path.expanduser(config['MAIN.BackupDir']),'current-'+db+'.sql.gz'))

            # backup was not successful - remove partially completed file and inc counter
            if retcode > 0:
                i += 1
            bresults[db] = 'backup error'
        if os.path.exists(bfile):
                    os.remove(bfile)
        else:
                # backup completed successfully - we have to reset couner for the next one if there are any and break 'while' cycle
                i = 3
            bresults[db] = 'backup ok'
        break

# prepare list of files for backup, which will be downloaded over sftp and list of directories, which will be created on destination host
def fs_backup(config):
    flist = os.path.join(os.path.expanduser(config['MAIN.BackupDir']), 'filelist')
    dlist = os.path.join(os.path.expanduser(config['MAIN.BackupDir']), 'dirlist')
    f = open(flist,'w')
    d = open(dlist,'w')
    for (dirpath, dirnames, filenames) in walk(os.path.expanduser(config['BACKUP.DirsToBackup'])):
        if filenames:
        d.write(dirpath+'\n')
            for fn in filenames:
                fpath = os.path.join(dirpath,fn)
                try:
            f.write(fpath+'|'+str(os.path.getsize(fpath))+'|'+str(os.path.getmtime(fpath))+'\n')
                except:
                    print('Could not get info for',fpath)
    f.close()
    d.close()

# purging old database backups according settings in configuration file
def purge(config):
    print("Removing database backup older than",config['PURGE.DaysToKeep'],"days")
    os.chdir(os.path.abspath(config['MAIN.BackupDir']))
    pb = [d for d in os.listdir('.') if os.path.isfile(d) and os.path.getmtime(os.path.abspath(d)) < time.time()-24*60*60*int(config['PURGE.DaysToKeep']) ]
    for d in pb:
        if re.search('gz$',d) != 'None':
            print("  Removing",d,"...")
            os.remove(os.path.abspath(d))

def main():
    config = init_config(sys.argv)
    dblist = mysql_dblist(config['MAIN.MySqlUserFile'])
    mysql_backup(config, dblist)
    fs_backup(config)
    purge(config)
 
if __name__ == '__main__':
    main()
