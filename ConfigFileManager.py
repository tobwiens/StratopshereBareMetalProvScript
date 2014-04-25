'''
Created on 23/12/2013

@author: Tobias Wiens
'''
import ConfigParser
import sys

class ConfigFileManager(object):
    '''
    This file is managing the config file which inherits all important configuration variables.
    The config file contains an overall section which contains:
    aws_key_id
    aws_acces_key
    region
    image id
    setup commands
    create setup image id with name
    '''

    #Global variables
    configPath = 'conf/instances.cfg'
    config = None
    
    #Section names
    OVERALL_SECTION_NAME = 'Basic'
    JOB_MANAGER_SECTION_NAME = 'JobManager'
    TASK_MANAGER_SECTION_NAME = 'TaskManager'
    STRATOSPHERE_SECTION_NAME = 'Stratosphere'
    
    #Overall section OPTIONS
    OVERALL_AWS_SECRET_KEY = 'aws secret key'
    OVERALL_AWS_KEY_ID = 'key id'
    OVERALL_IMAGE_ID = 'image id'
    OVERALL_SETUP_COMMANDS = 'setup commands'
    OVERALL_SETUP_IMAGE_NAME = 'Save setup instance as'
    OVERALL_REGION = 'region'
    OVERALL_IPACCES = 'IP access'
    OVERALL_USERNAME = 'username'
    OVERALL_JAVA_HOME = 'java home directory'
    OVERALL_USER_DATA_FILE = 'user-data-file'
    
    #JobManager section OPTIONS
    JOB_MANAGER_SECURITY_GROUP = 'security group'
    JOB_MANAGER_KEY_NAME = 'key name'
    JOB_MANAGER_KEY_PATH = 'key path'
    JOB_MANAGER_OVERWRITE_KEY = 'create key'
    JOB_MANAGER_INSTANCE_TYPE = 'instance type'
    JOB_MANAGER_SECURITY_GROUP = 'security group'
    
    #TaskManager section OPTIONS
    TASK_MANAGER_SECURITY_GROUP = 'security group'
    TASK_MANAGER_KEY_NAME = 'key name'
    TASK_MANAGER_KEY_PATH = 'key path'
    TASK_MANAGER_OVERWRITE_KEY = 'create key'
    TASK_MANAGER_INSTANCE_TYPE = 'instance type'
    TASK_MANAGER_INSTANCE_COUNT = 'instance count'
    TASK_MANAGER_SECURITY_GROUP = 'security group'
    
    #Stratosphere section options
    STRATOPSHERE_PATH = 'path'
    
    def __init__(self, configPath=None):
        '''
        Constructor:
        Create config file. If file does not exist a new empty configuraion file will be created
        @param configPath: /path/to/file/file.config
        @type configPath: String
        '''
        if configPath is not None:
            self.configPath = configPath
        
        #create config object
        self.config = ConfigParser.ConfigParser()
        try:
            openConfigFile = file(self.configPath, "r")
            self.config.readfp(fp=openConfigFile)
        except IOError as e:
            print e
            print 'Create new config file'
            self.createEmptyConfigFile()
            print 'Empty config file can be found at '+self.configPath
            sys.exit('Empty config file was created... Please customise it!')
            
         
       
    def createEmptyConfigFile(self):
        openConfigFile = file(self.configPath, 'w')
        
        #add sections
        self.config.add_section(self.OVERALL_SECTION_NAME)
        self.config.add_section(self.JOB_MANAGER_SECTION_NAME)
        self.config.add_section(self.TASK_MANAGER_SECTION_NAME)
        self.config.add_section(self.STRATOSPHERE_SECTION_NAME)
        
        #add attributes
        self.config.set(section=self.OVERALL_SECTION_NAME, option=self.OVERALL_AWS_SECRET_KEY, value=None)
        self.config.set(section=self.OVERALL_SECTION_NAME, option=self.OVERALL_AWS_KEY_ID, value=None)
        self.config.set(section=self.OVERALL_SECTION_NAME, option=self.OVERALL_REGION, value=None)
        self.config.set(section=self.OVERALL_SECTION_NAME, option=self.OVERALL_IMAGE_ID, value=None)
        self.config.set(section=self.OVERALL_SECTION_NAME, option=self.OVERALL_USERNAME, value=None)
        self.config.set(section=self.OVERALL_SECTION_NAME, option=self.OVERALL_SETUP_COMMANDS, value=None)
        self.config.set(section=self.OVERALL_SECTION_NAME, option=self.OVERALL_SETUP_IMAGE_NAME, value=None)
        self.config.set(section=self.OVERALL_SECTION_NAME, option=self.OVERALL_IPACCES, value='0.0.0.0/0')
        self.config.set(section=self.OVERALL_SECTION_NAME, option=self.OVERALL_JAVA_HOME, value = '/path/to/java_home/')
        self.config.set(section=self.OVERALL_SECTION_NAME, option=self.OVERALL_USER_DATA_FILE, value = '/path/to/user-data/user-data.sh')
        
        #job manager
        self.config.set(section=self.JOB_MANAGER_SECTION_NAME, option=self.JOB_MANAGER_SECURITY_GROUP, value=None)
        self.config.set(section=self.JOB_MANAGER_SECTION_NAME, option=self.JOB_MANAGER_KEY_NAME, value=None)
        self.config.set(section=self.JOB_MANAGER_SECTION_NAME, option=self.JOB_MANAGER_KEY_PATH, value=None)
        self.config.set(section=self.JOB_MANAGER_SECTION_NAME, option=self.JOB_MANAGER_OVERWRITE_KEY, value=None)
        self.config.set(section=self.JOB_MANAGER_SECTION_NAME, option=self.JOB_MANAGER_INSTANCE_TYPE, value=None)
        self.config.set(section=self.JOB_MANAGER_SECTION_NAME, option=self.JOB_MANAGER_SECURITY_GROUP, value=None)
        
        #task manager
        self.config.set(section=self.TASK_MANAGER_SECTION_NAME, option=self.TASK_MANAGER_SECURITY_GROUP, value=None)
        self.config.set(section=self.TASK_MANAGER_SECTION_NAME, option=self.TASK_MANAGER_KEY_NAME, value=None)
        self.config.set(section=self.TASK_MANAGER_SECTION_NAME, option=self.TASK_MANAGER_KEY_PATH, value=None)
        self.config.set(section=self.TASK_MANAGER_SECTION_NAME, option=self.TASK_MANAGER_OVERWRITE_KEY, value=None)
        self.config.set(section=self.TASK_MANAGER_SECTION_NAME, option=self.TASK_MANAGER_INSTANCE_TYPE, value=None)
        self.config.set(section=self.TASK_MANAGER_SECTION_NAME, option=self.TASK_MANAGER_INSTANCE_COUNT, value=None)
        self.config.set(section=self.TASK_MANAGER_SECTION_NAME, option=self.TASK_MANAGER_SECURITY_GROUP, value=None)
        
        #stratosphere
        self.config.set(section=self.STRATOSPHERE_SECTION_NAME, option=self.STRATOPSHERE_PATH, value='/home/ubuntu/stratosphere/')
        
        #write and close
        self.config.write(openConfigFile)
        openConfigFile.close()
        
    def getAWSAccesKey(self):
        return self.config.get(section=self.OVERALL_SECTION_NAME, option=self.OVERALL_AWS_SECRET_KEY)
    
    def getAWSKeyID(self):
        return self.config.get(section=self.OVERALL_SECTION_NAME, option=self.OVERALL_AWS_KEY_ID)
    
    def getImageId(self):
        return self.config.get(section=self.OVERALL_SECTION_NAME, option=self.OVERALL_IMAGE_ID)
    
    def getRegion(self):
        return self.config.get(section=self.OVERALL_SECTION_NAME, option=self.OVERALL_REGION)
    
    def getJobManagerKeyName(self):
        return self.config.get(section=self.JOB_MANAGER_SECTION_NAME, option=self.JOB_MANAGER_KEY_NAME)
    
    def getJobManagerSaveKey(self):
        return self.config.get(section=self.JOB_MANAGER_SECTION_NAME, option=self.JOB_MANAGER_OVERWRITE_KEY).lower() in ['true']
    
    def getTaskManagerSaveKey(self):
        return self.config.get(section=self.TASK_MANAGER_SECTION_NAME, option=self.TASK_MANAGER_OVERWRITE_KEY).lower() in ['true']
    
    def getJobManagerKeyPath(self):
        return self.config.get(section=self.JOB_MANAGER_SECTION_NAME, option=self.JOB_MANAGER_KEY_PATH)
    
    def getJobManagerInstanceType(self):
        return self.config.get(section=self.JOB_MANAGER_SECTION_NAME, option=self.JOB_MANAGER_INSTANCE_TYPE)
    
    def getTaskManagerInstanceType(self):
        return self.config.get(section=self.TASK_MANAGER_SECTION_NAME, option=self.TASK_MANAGER_INSTANCE_TYPE)
    
    def getTaskManagerInstanceCount(self):
        return self.config.get(section=self.TASK_MANAGER_SECTION_NAME, option=self.TASK_MANAGER_INSTANCE_COUNT)
    
    def getJobManagerSecurityGroup(self):
        return self.config.get(section=self.JOB_MANAGER_SECTION_NAME, option=self.JOB_MANAGER_SECURITY_GROUP)
    
    def getTaskManagerSecurityGroup(self):
        return self.config.get(section=self.TASK_MANAGER_SECTION_NAME, option=self.TASK_MANAGER_SECURITY_GROUP)
    
    def getCommandList(self):
        return self.config.get(section=self.OVERALL_SECTION_NAME, option=self.OVERALL_SETUP_COMMANDS)
    
    def getIPAccess(self):
        return self.config.get(section=self.OVERALL_SECTION_NAME, option=self.OVERALL_IPACCES)
    
    def getUsername(self):
        return self.config.get(section=self.OVERALL_SECTION_NAME, option=self.OVERALL_USERNAME)
    
    def getStratospherePath(self):
        return self.config.get(section=self.STRATOSPHERE_SECTION_NAME, option=self.STRATOPSHERE_PATH)
    
    def getTaskManagerKeyName(self):
        return self.config.get(section=self.TASK_MANAGER_SECTION_NAME, option=self.TASK_MANAGER_KEY_NAME)
    
    def getJavaHome(self):
        return self.config.get(section=self.OVERALL_SECTION_NAME, option=self.OVERALL_JAVA_HOME)
    
    def getUserDataFile(self):
        return self.config.get(section=self.OVERALL_SECTION_NAME, option=self.OVERALL_USER_DATA_FILE)
    