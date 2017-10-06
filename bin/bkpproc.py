#!/usr/bin/env python3

import sys
import os
import datetime
import subprocess
import re
import shutil
import smtplib


def run_command(cmd):
    try:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
    except:
        print("Could not run command", cmd, sys.exc_info()[0])
        sys.exit(1)
    return p.returncode


def ldownload(tuser, tport, tdomain):
    print('Executing remote preparation script')
    cmd = ['ssh', '-p', tport, '-C', tuser+'@' + tdomain, 'python3 ~/sitebackup/bin/mysqlbkp.py']
    rc = run_command(cmd)
    if rc != 0:
        print("Remote execution returned non zero code! Exiting...", sys.exc_info())
        sys.exit(1)

    print('Getting list of directories')
    cmd = ['scp', '-P', tport, '-C', tuser + '@' + tdomain + ':~/sitebackup/site/dirlist', '.']
    rc = run_command(cmd)
    if rc != 0:
        print("Transfer of dirlist was not successful and returned non zero code! Exiting...")
        sys.exit(1)

    print('Getting list of files to backup')
    cmd = ['scp', '-P', tport, '-C', tuser+'@'+tdomain+':~/sitebackup/site/filelist', '.']
    rc = run_command(cmd)
    if rc != 0:     
        print("Transfer of filelist was not successful and returned non zero code! Exiting...")
        sys.exit(1)

    print('Getting current mysql backups')
    cmd = ['scp', '-P', tport, '-C', tuser+'@' + tdomain + ':~/sitebackup/site/current-*.sql.gz', '.']
    rc = run_command(cmd)
    if rc != 0:
        print("Transfer of database backups was not successful and returned non zero code! Exiting...")
        sys.exit(1)


def prepare(cdir):
    
    # find previous backup list
    os.chdir(os.path.abspath(os.path.join(cdir, '..')))

    if len(os.listdir(os.getcwd())) > 1:
        pb = max([d for d in os.listdir('.') if os.path.isdir(d) and os.path.abspath(d) != cdir], key=os.path.getmtime)
        print('Previous backup directory:', pb)
        if os.access(os.path.join(os.path.abspath(os.path.join(cdir, '..', pb)), 'filelist'), os.R_OK):
            pflist = os.path.join(os.path.abspath(os.path.join(cdir, '..', pb)), 'filelist')
            print('Previous backup filelist:', pflist)
        else:
            print('Privous backup filelist does not exist')
            pb = ''
    else:
        pb = ''
        print('Previous backup does not exist')

    dlist = os.path.join(cdir,'dirlist')
    flist = os.path.join(cdir,'filelist')
    delta = os.path.join(cdir,'delta')

    # ======================================================================================

    if pb:
        # set of files which exist in new, but not in prev
        with open(flist, 'r') as file1:
            with open(pflist, 'r') as file2:
                diff = set(file1).difference(file2)

        diff.discard('\n')

        # set of files which exists in both
        with open(flist, 'r') as file1:
            with open(pflist, 'r') as file2:
                same = set(file1).intersection(file2)

        same.discard('\n')

        # set of files which exist in prev, but not in new, missing
        with open(flist, 'r') as file1:
            with open(pflist, 'r') as file2:
                miss = set(file2).difference(file1)

        miss.discard('\n')

    # ======================================================================================

    # create directories for all files
    print('Creation of local directories')
    with open(dlist, 'r') as d:
        for pth in d.readlines():
            os.makedirs(cdir + pth.strip('\n'))

    # if previous backup exists
    if pb:
        print('Delta generation using data from previous backup')
        # create delta files which exist in new filelist, ignore temporary files
        # and file which were already copied in previous attempts
        with open(delta, 'w') as file_out:
            for line in diff:
                if not re.search('/tmp/|/cache/|/thumbnails/', line.split('|')[0]) and not os.path.exists(os.path.join(cdir, line.split('|')[0]).lstrip(os.path.sep)):
                    file_out.write(line.split('|')[0]+'\n')

        # add files which exist in both filelists, but could not be found in prev backup on fs, ignore temporary files
        # and file which were already copied in previous attempts
        with open(delta, 'a') as file_out:
            for line in same:
                if not os.access(os.path.join(cdir,'..', pb, line.split('|')[0].lstrip(os.path.sep)), os.R_OK):
                    if not re.search('/tmp/|/cache/|/thumbnails/',line.split('|')[0]) and not os.path.exists(os.path.join(cdir,line.split('|')[0]).lstrip(os.path.sep)):
                        file_out.write(line.split('|')[0] + '\n')
                else:
                    if not os.path.exists(os.path.join(cdir, line.split('|')[0]).lstrip(os.path.sep)):
                        shutil.copy(os.path.join(cdir, '..', pb, line.split('|')[0].lstrip(os.path.sep)), os.path.join(cdir, line.split('|')[0].lstrip(os.path.sep)))

    # if there were no previos backups and we have to build it from scratch
    # and file which were already copied in previous attempts
    else:
        print('Delta generated using current filelist')
        with open(flist, 'r') as file1:
            with open(delta, 'w') as file_out:
                for line in file1:
                    # everything except temporary files will be backed up
                    if not re.search('/tmp/|/cache/|/thumbnails/', line.split('|')[0]) and not os.path.exists(os.path.join(cdir, line.split('|')[0].lstrip(os.path.sep))):
                        file_out.write(line.split('|')[0] + '\n')


def sync(cdir, tuser, tport, tdomain):

    print('Push delta to target system')
    delta = os.path.join(cdir, 'delta')

    # push delta to target system
    cmd = ['scp', '-P', tport, os.path.join(cdir, 'delta'), tuser+'@'+tdomain+':~/sitebackup/site/delta']

    try:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
    except:
        print("Could not push delta due to error :", sys.exc_info())
        sys.exit(1)
 
    if p.returncode > 0:
        print("Delta push process was unexpectedly terminated")

    os.chdir(cdir)

    print("Get delta files from target system")

    # get files from delta back to backup system
    cat = ['tar', 'xfz', '-']
    ssh = ['ssh', '-p', tport, tuser + '@' + tdomain, 'tar cfz - -T ~/sitebackup/site/delta']

    sshc = subprocess.Popen(ssh, stdout=subprocess.PIPE, stderr=sys.stdout)
    catc = subprocess.call(cat, stdin=sshc.stdout, stdout=sys.stdout, stderr=sys.stdout)

    if catc > 0:
        print("Issue with transfer and writing files on local drive")

    return catc


def purge(cdir, tpurge):
    print("Removing folders older than", tpurge, "days relatevely to", cdir)
    os.chdir(os.path.abspath(os.path.join(cdir, '..')))
    bkpdir = os.getcwd()
    pb = [d for d in os.listdir('.') if os.path.isdir(d) and os.path.abspath(d) != cdir and os.path.getmtime(os.path.abspath(d)) < os.path.getmtime(os.path.abspath(cdir))-24*60*60*tpurge ]
    for d in pb:
        print("  Removing", d, "...")
        shutil.rmtree(os.path.abspath(d))


def report(subject, body):
    sender = 'webbackup.p3w@gmail.com'
    password = 'v6bzQEZisdiq5jKpYaTI'
    to = ['webbackup.p3w@gmail.com']
    message = """\
From: %s
To: %s
Subject: %s

%s
""" % (sender, ','.join(to), subject, body)

    try:
        smtp = smtplib.SMTP('smtp.gmail.com', 587)
        smtp.starttls()
        smtp.login(sender, password)
        smtp.sendmail(sender, to, message)
        print("Successfully sent email")
    except:
        print("Error: unable to send email", sys.exc_info())


def repeatSync(cdir, tuser, tdomain):
    print("Not implemented yet")


def main():

    if '--dir' in sys.argv:
        bkproot = sys.argv[sys.argv.index('--dir') + 1]
    else:
        bkproot = '/data/WebBackup'

    if '--user' in sys.argv:
        tuser = sys.argv[sys.argv.index('--user') + 1]
    else:
        print('No --user defined')
        sys.exit(1)

    if '--domain' in sys.argv:
        tdomain = sys.argv[sys.argv.index('--domain') + 1]
    else:
        print('No --domain defined')
        sys.exit(1)

    if '--port' in sys.argv:
        tport = str(sys.argv[sys.argv.index('--port') + 1])
    else:
        tport = 22

    if '--purge' in sys.argv:
        tpurge = int(sys.argv[sys.argv.index('--purge') + 1])
    else:
        tpurge = ''

    cdir = os.path.join(bkproot, tdomain, datetime.datetime.now().strftime('%Y%m%d%H%M'))
    dlist = os.path.join(cdir, 'dirlist')
    flist = os.path.join(cdir, 'filelist')

    if not os.access(cdir, os.W_OK):
        os.makedirs(cdir)

    os.chdir(cdir)
    print("Current directory:", cdir)
    ldownload(tuser, tport, tdomain)

    prepare(cdir)

    while sync(cdir, tuser, tport, tdomain) > 0:
        print("Non zero sync responce, repeating prepare")
        prepare(cdir)

    if tpurge:
        purge(cdir, tpurge)
   
    report("Report for " + tdomain + " completed", 'eof')

if __name__ == '__main__':
    main()
