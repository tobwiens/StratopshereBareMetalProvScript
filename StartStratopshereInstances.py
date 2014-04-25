'''
Created on 23/12/2013

This script is starting ec2 instances.

@author: Tobias Wiens
'''
MAXIMUM_TRIES = 100

from ec2SetupScript import ConfigFileManager, BotoConnectionManager, StratosphereSettings
import sys, time, paramiko, StringIO, socket
from ec2SetupScript import AmazonSSHUtils


def setupInstance(keyMaterial, commandList, instance, username):
    """
    Waits for the instance to be ready to connect via SSH to. All commands specified in commandList are then executed.
    @param keyMaterial: the private key material as a string to connect via SSH
    @type keyMaterial: String
    @param commandList: Array of commands which will be executed in order and only if preceding command finished
    @type commandList: Array of strings
    @param instance: Amazon ec2 instance on which to execute the commands on
    @type instance: Boto instance, which results out of an run instances request
    @param username: Username to log into the machine
    @type username: String        
    """
    result = False
    #check if instance is running
    while instance.state != 'running':
        instance.update()
        time.sleep(2)
    #instance is now running
    print 'Instance is running!Needs time to boot...'
    print instance.ip_address  
    print instance.id

    clientSSH = paramiko.SSHClient()
    materialOpenFile = StringIO.StringIO(keyMaterial)
    instanceKey = paramiko.RSAKey(file_obj=materialOpenFile)
    #add host automatically
    clientSSH.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    for i in range(MAXIMUM_TRIES):
        try:
            clientSSH.connect(hostname=instance.ip_address, port=22, username=username, pkey=instanceKey)
        except paramiko.AuthenticationException as e:
            print e
        except socket.error as e:
            print e
        except:
            print e
        else:
            #setup instance
            for command in commandList:
                AmazonSSHUtils.executeCommand(command=command, sshClient=clientSSH)
            result = True
            break #break the for loop
    else:
        #This else is executed when the for loop was NOT terminated by a break
        print 'Could NOT connect to instance'
        
    clientSSH.close()
    return result

def getKeyMaterial(keyName, keyPath, amazonConnection, createKey=False):
    '''
    Tries to get the key material from hard disk or creates another keyPair and saves it on hard disk
    @param keyName: Name of the key in amazon ec2
    @type keyName: String
    @param keyPath: Local path and file name of the key /path/to/key/file.pem for example
    @type keyPath: String
    @param amazonConnection: Boto established connection to amazon region. It is checked if the key 
    exists already in that region, or it is used for key creation
    @type amazonConnection: Boto region connection
    @param createKey: specifies if the key should be created at the amazon region and the local file be overwritten with
    that new key.
    @type createKey: True or False (default), to prevent losing important key files on hard disk  
    '''
    if createKey is True:
        try:
            setupKey = amazonConnection.ec2RegionConnection.create_key_pair(key_name=keyName)
        except:
            sys.exit('Key '+keyName+' does already exist. Please change configuration file!')
        try:
            openKeyFile = file(keyPath, "w")
            openKeyFile.writelines(setupKey.material)
            openKeyFile.close()
            return setupKey.material
        except:
            print 'Could not save key to disk'
            sys.exit('Please make sure that the given path exists '+keyPath)
    else:
        openKeyFile = file(keyPath, 'r') 
        try:
            #create empty string and concatinate lines
            result = ''
            for line in openKeyFile:
                result += line
            openKeyFile.close()
            return result
        except:
            print 'Key pair '+keyName+' does not exist in Amazon aws' 

if __name__ == '__main__':
    print 'Stratosphere EC2 Deployment'
    
    #To connect to amazon we need to read the region which to connect to, out of the config file
    configFile = ConfigFileManager.ConfigFileManager()
    print 'Config file opened'
    
    #Connect to amazon region
    amazonConnection = BotoConnectionManager.BotoConnectionManager(aws_secret_key=configFile.getAWSAccesKey(), aws_key_id=configFile.getAWSKeyID(), region=configFile.getRegion())
    print "Successfully connected to region "+configFile.getRegion()
    
    #authorizeSSH access - Job manager
    if AmazonSSHUtils.authorizeSSH(securityGroupName = configFile.getJobManagerSecurityGroup(), amazonConnection = amazonConnection, ipAddress=configFile.getIPAccess()) is False:
        print 'Security group '+configFile.getJobManagerSecurityGroup()+' not found'
        sys.exit('Update config file')
    #task manager  
    if AmazonSSHUtils.authorizeSSH(securityGroupName =configFile.getTaskManagerSecurityGroup(), amazonConnection = amazonConnection, ipAddress=configFile.getIPAccess()) is False:
        print 'Security group '+configFile.getTaskManagerSecurityGroup()+' not found'
        sys.exit('Update config file')
    
    #Get secret SSH key
    keyMaterial = getKeyMaterial(keyName=configFile.getJobManagerKeyName(), keyPath=configFile.getJobManagerKeyPath(), amazonConnection=amazonConnection, createKey=configFile.getJobManagerSaveKey())

    print 'Start one instance of type '+configFile.getJobManagerInstanceType()+' to run the Job Manager with image id: '+configFile.getImageId()
    jobManagerReservation = amazonConnection.ec2RegionConnection.run_instances(image_id=configFile.getImageId(),instance_type=configFile.getJobManagerInstanceType(), key_name=configFile.getJobManagerKeyName(), security_groups=[configFile.getJobManagerSecurityGroup()], dry_run=False)
    
    setupInstance(keyMaterial=keyMaterial, commandList=configFile.getCommandList().split(','), instance=jobManagerReservation.instances[0], username=configFile.getUsername())
    
    #save public and private ip address
    jobManagerPublicIP = jobManagerReservation.instances[0].ip_address
    jobManagerPrivateIP = jobManagerReservation.instances[0].private_ip_address
    
    numberOfTaskManagers = configFile.getTaskManagerInstanceCount()
    print 'Start '+str(numberOfTaskManagers)+' instance of type '+configFile.getTaskManagerInstanceType()+' to run the Job Manager with image id: '+configFile.getImageId()
    if int(numberOfTaskManagers) > 0:
        taskManagerReservation = amazonConnection.ec2RegionConnection.run_instances(image_id=configFile.getImageId(),instance_type=configFile.getTaskManagerInstanceType(), key_name=configFile.getTaskManagerKeyName(), security_groups=[configFile.getTaskManagerSecurityGroup()], min_count=numberOfTaskManagers, max_count=numberOfTaskManagers, dry_run=False)

    
    #save private ip addresses for task managers
    taskManagerPrivateIpList = []
    taskManagerPublicIpList = []
    
    if int(numberOfTaskManagers) > 0:
        for instance in taskManagerReservation.instances:
            setupInstance(keyMaterial=keyMaterial, commandList=configFile.getCommandList().split(','), instance=instance, username=configFile.getUsername())
            taskManagerPrivateIpList.append(instance.private_ip_address)
            taskManagerPublicIpList.append(instance.ip_address)
        
    StratosphereSettings.StratosphereSettings(stratospherePath=configFile.getStratospherePath(), taskManagerPrivateIPList=taskManagerPrivateIpList,taskManagerPublicIPList=taskManagerPublicIpList, jobManagerPrivateIP=jobManagerPrivateIP, jobManagerPublicIP=jobManagerPublicIP, keyMaterial=keyMaterial, username=configFile.getUsername(), javaHomePath=configFile.getJavaHome())
    
    
    time.sleep(50*60)
    
    #Revoke given SSH access
    AmazonSSHUtils.revokeSSH(securityGroupName =configFile.getTaskManagerSecurityGroup(), amazonConnection = amazonConnection, ipAddress=configFile.getIPAccess())
    #revokeSSH(securityGroupName =configFile.getJobManagerSecurityGroup(), amazonConnection = amazonConnection, ipAddress=configFile.getIPAccess())
    