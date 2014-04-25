'''
Created on 17/01/2014

@author: Tobias Wiens
'''

import thread, paramiko, socket, StringIO, os, time, AmazonSSHUtils

class StratosphereSettings(object):
    '''
    This class sets settings in the stratosphere cluster. 
    
    It connects via SSH to a node and sets master and slave nodes in stratopshere's configuration files.
    '''
    
    #list of all public IPS
    configureIP = []
    #more specified IP lists
    taskManagerPrivateIPList = []
    taskManagerPublicIPList = []
    jobManagerPrivateIP = None
    jobManagerPublicIP = None
    
    stratospherePath = None
    keyMaterial = None
    username = None
    stratospherePath = None
    javaHomePath = None
    
    finishCounterLock = None
    finishedCount = 0
    
    MAXIMUM_TRIES = 50;
    
    def increaseFinishCount(self):
        with self.finishCounterLock:
            self.finishedCount+=1
    
    '''
    
    '''
    def writeJobManager(self, ipAddress):
        clientSSH = paramiko.SSHClient()
        materialOpenFile = StringIO.StringIO(self.keyMaterial)
        instanceKey = paramiko.RSAKey(file_obj=materialOpenFile)
        #add host automatically
        clientSSH.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        for i in range(self.MAXIMUM_TRIES):
            try:
                clientSSH.connect(hostname=ipAddress, port=22, username=self.username, pkey=instanceKey)
            except paramiko.AuthenticationException as e:
                    print str(e)+' IP:'+str(ipAddress)
            except socket.error as e:
                    print str(e)+' IP:'+str(ipAddress)
            except:
                    print 'Unknown error occured during connecting to '+str(ipAddress)
            else:
                #get configuration file
                print self.stratospherePath+'conf/stratosphere-conf.yaml'+ipAddress
                #try:
                clientSSH.open_sftp().get(self.stratospherePath+'conf/stratosphere-conf.yaml', 'stratosphere-conf.yaml'+ipAddress)
                #except:
                #    print 'FAILED to write the configuration file of '+ipAddress+' stratosphere-conf.yaml'
                openConfigFile = open('stratosphere-conf.yaml'+str(ipAddress), 'r')
                openAlteredFile = open('stratosphere-conf-withIpReplaced.yaml'+ipAddress, 'wb')
                
                outputString = ''
                #pipeline file and replace needed part
                
                #print 'Count :'+str(complete.count('\n'))
                for line in openConfigFile.read().splitlines():
                    if 'jobmanager.rpc.address' in line:
                        line = 'jobmanager.rpc.address: '+self.jobManagerPrivateIP
                    if 'env.java.home' in line:
                        line = 'env.java.home: '+self.javaHomePath
                    openAlteredFile.write(line+'\n')
                
                #close files
                openConfigFile.close()
                openAlteredFile.close()
                
                #transfer altered file back
                clientSSH.open_sftp().put('stratosphere-conf-withIpReplaced.yaml'+ipAddress,self.stratospherePath+'conf/stratosphere-conf.yaml')
                
                #remove files
                os.remove('stratosphere-conf.yaml'+ipAddress)
                os.remove('stratosphere-conf-withIpReplaced.yaml'+ipAddress)
                
                #break the trying for loop
                self.increaseFinishCount()
                break;
            
        #close the connection
        clientSSH.close()
                    
                
        

    def writeSlavesFile(self, ipAddress):
        clientSSH = paramiko.SSHClient()
        materialOpenFile = StringIO.StringIO(self.keyMaterial)
        instanceKey = paramiko.RSAKey(file_obj=materialOpenFile)
        #add host automatically
        clientSSH.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        for i in range(self.MAXIMUM_TRIES):
            try:
                clientSSH.connect(hostname=ipAddress, port=22, username=self.username, pkey=instanceKey)
            except paramiko.AuthenticationException as e:
                    print str(e)+' IP:'+str(ipAddress)
            except socket.error as e:
                    print str(e)+' IP:'+str(ipAddress)
            except:
                    print 'Unknown error occured during connecting to '+str(ipAddress)
            else:
                #write slaves file
                sftp = clientSSH.open_sftp()
                try:
                    sftp.put('slaves',self.stratospherePath+'conf/slaves')
                finally:
                    sftp.close()
                
                #break the trying for loop
                self.increaseFinishCount()
                break
         
        #close the connection
        clientSSH.close()

   
    def getStatus(self):
        '''
        @return: True if all nodes have been set up successfully, false otherwise.
        '''
        print self.finishedCount
        if self.finishedCount is len(self.configureIP)*2:
            return True
        else:
            return False
       
    def startTaskOrJobManager(self, ipAddress, startJobManager = False):
        clientSSH = paramiko.SSHClient()
        materialOpenFile = StringIO.StringIO(self.keyMaterial)
        instanceKey = paramiko.RSAKey(file_obj=materialOpenFile)
        #add host automatically
        clientSSH.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        for i in range(self.MAXIMUM_TRIES):
            try:
                clientSSH.connect(hostname=ipAddress, port=22, username=self.username, pkey=instanceKey)
            except paramiko.AuthenticationException as e:
                    print str(e)+' IP:'+str(ipAddress)
            except socket.error as e:
                    print str(e)+' IP:'+str(ipAddress)
            except:
                    print 'Unknown error occured during connecting to '+str(ipAddress)
            else:
                if startJobManager is True:
                    AmazonSSHUtils.executeCommand(command= 'cd '+self.stratospherePath+'bin;'+'./nephele-jobmanager.sh start cluster', sshClient= clientSSH)
                else:
                    AmazonSSHUtils.executeCommand(command= 'cd '+self.stratospherePath+'bin;'+'./nephele-taskmanager.sh start', sshClient= clientSSH)
                #break the trying for loop
                break
       
        #close the connection
        clientSSH.close()
        
        
    def startClusterSetup(self):
        print 'Start task managers'
        for taskManager in self.taskManagerPublicIPList:
            print 'Start: '+taskManager
            self.startTaskOrJobManager(ipAddress = taskManager)
        
        print "Start job manager: "+self.jobManagerPublicIP
        self.startTaskOrJobManager(ipAddress=self.jobManagerPublicIP, startJobManager = True)
        
            
            
        
    def __init__(self, stratospherePath, taskManagerPrivateIPList, taskManagerPublicIPList, jobManagerPrivateIP, jobManagerPublicIP, keyMaterial, username, javaHomePath):
        '''
        Constructor: Saves all values and starts threads for each IP in the configuration list.
        When all nodes are successfully initialized it will be recognized by this class.
        @param stratospherePath: defines where statopshere is installed
        @param traskManagerIPList: holds all private IP addresses from the taskManager nodes
        @param jobManagerIP: holds the private IP address of the job manager
        @param keyMaterial: key material for connecting via SSH
        @param username: username to authentificate with
        @param IPAddressList: IP addresses to connect with and write stratosphere configureation files   
        '''
        #for multiphtreading acquire a lock
        self.finishCounterLock = thread.allocate_lock()
        
        #Save all values
        self.stratospherePath = stratospherePath
        self.taskManagerPrivateIPList = taskManagerPrivateIPList
        self.jobManagerPrivateIP = jobManagerPrivateIP
        self.taskManagerPublicIPList = taskManagerPublicIPList
        self.jobManagerPublicIP = jobManagerPublicIP
        
        self.keyMaterial = keyMaterial
        self.username = username
        self.stratospherePath = stratospherePath
        
        #list of all nodes public IPs
        self.configureIP = []
        for taskManager in taskManagerPublicIPList:
            self.configureIP.append(taskManager)
        self.configureIP.append(jobManagerPublicIP)
        self.javaHomePath = javaHomePath
         
        #create local slaves file and save it to disk   
        openSlavesFile = file("slaves", 'w')
        for slave in taskManagerPrivateIPList:
            openSlavesFile.write(slave+'\n')
        openSlavesFile.close()
        
        for node in self.configureIP:
            thread.start_new_thread(self.writeSlavesFile, (node, ) )
            thread.start_new_thread(self.writeJobManager, (node, ) )
            
        for i in range(20):
            if self.getStatus() is True:
                print 'Cluster is set up'
                self.startClusterSetup()
                break
            else:
                print 'Cluster is not ready yet.... wait'
                time.sleep(5)
                
        #remove the last files:
        #remove slaves file  
        os.remove('slaves')   
        