import gemprops
import netifaces
import os
import socket


class ClusterDef:
    
    def __init__(self, cdef):
        self.clusterDef = cdef
        self.thisHost = socket.gethostname()

     
    def hostName(self):
        return self.thisHost
        
    def isBindAddressProperty(self, propName):
        if propName.endswith('bind-address'):
            return True
        
        if propName.endswith('BIND_ADDRESS'):
            return True
        
        return False

    

    def translateBindAddress(self,addr):
        if not '.' in addr:
            if addr in netifaces.interfaces():
                #TODO does this ever return an ipV6 address ?  Is that a problem ?
                return netifaces.ifaddresses(addr)[netifaces.AF_INET][0]['addr']
            else:
                # in this case, assume it is a host name
                return addr
        else:
            return addr


    def isProcessOnThisHost(self, processName, processType):
        result = False
        for hostname in [self.thisHost,'localhost']:
            if hostname in self.clusterDef['hosts']:
                if processName in self.clusterDef['hosts'][hostname]['processes']:
                    process = self.clusterDef['hosts'][hostname]['processes'][processName]
                    result =  process['type'] == processType
                    break
                        
        return result
        

    def processProps(self, processName, host=None):
        processes = None
        if host is None:
            thishost = self.thisHost
        else:
            thishost = host
            
        if thishost in self.clusterDef['hosts']:
            processes = self.clusterDef['hosts'][thishost]['processes']
        
        elif host is None and 'localhost' in self.clusterDef['hosts']:
            processes = self.clusterDef['hosts']['localhost']['processes']
            
        else:
            raise Exception('this host ({0}) not found in cluster definition'.format(thishost))
                    
        return processes[processName]


    def hostProps(self, host = None):
        result = dict()
        
        if host is None:
            thishost = self.thisHost
        else:
            thishost = host
        
        if thishost in self.clusterDef['hosts']:
            result = self.clusterDef['hosts'][thishost]['host-properties']
            
        elif host is None and 'localhost'  in self.clusterDef['hosts']:
            result = self.clusterDef['hosts']['localhost']['host-properties']
        
        return result
        

    def props(self, scope):
        if scope in self.clusterDef:
            return self.clusterDef[scope]
        else:
            return dict()    

    def processProperty(self, processType, processName, propertyName, host = None):
        pProps = self.processProps(processName, host = host)
        if propertyName in pProps:
            return pProps[propertyName]
        
        hostProps = self.hostProps(host = host)
        if propertyName in hostProps:
            return hostProps[propertyName]
        
        locProps = self.props(processType + '-properties')
        if propertyName in locProps:
            return locProps[propertyName]
        
        globProps = self.props('global-properties')
        if propertyName in globProps:
            return globProps[propertyName]
        else:
            raise Exception('property not found: ' + propertyName)



    def gfshArg(self, key, val):
        if self.isBindAddressProperty(key):
            val = self.translateBindAddress(val)
            
        if key in gemprops.GEMFIRE_PROPS:
            return '--J="-Dgemfire.{0}={1}"'.format(key,val)

        else:
            return '--J="-D{0}={1}"'.format(key,val)


    def buildGfshArgs(self, props):
        result = []
        for key in props.keys():
            if not key in gemprops.HANDLED_PROPS:
                result.append(self.gfshArg(key, props[key]))
                
        return result
                            
    def processesOnThisHost(self, processType):
        result = []
        for hostname in [self.thisHost, 'localhost']:        
            if hostname in self.clusterDef['hosts']:        
                for processName in self.clusterDef['hosts'][hostname]['processes'].keys():
                    process = self.clusterDef['hosts'][hostname]['processes'][processName]
                    if process['type'] == processType:
                        result.append(processName)
            
        return result


    def locatorsOnThisHost(self):
        return self.processesOnThisHost('locator')


    def datanodesOnThisHost(self):
        return self.processesOnThisHost('datanode')


    def isLocatorOnThisHost(self, processName):
        return self.isProcessOnThisHost(processName, 'locator')

    
    def isDatanodeOnThisHost(self, processName):
        return self.isProcessOnThisHost(processName, 'datanode')



    def locatorProperty(self, processName, propertyName, host=None):
        result = self.processProperty('locator',processName, propertyName, host = host)
        if self.isBindAddressProperty(propertyName):
            return self.translateBindAddress(result)
        else:
            return result


    def datanodeProperty(self, processName, propertyName, host=None):
        result = self.processProperty('datanode',processName, propertyName, host = host)
        if self.isBindAddressProperty(propertyName):
            return self.translateBindAddress(result)
        else:
            return result
        

    def hasDatanodeProperty(self, processName, propertyName, host = None):
        pProps = self.processProps(processName, host = host)
        if propertyName in pProps:
            return True
        
        hostProps = self.hostProps(host = host)
        if propertyName in hostProps:
            return True
        
        dnProps = self.props('datanode-properties')
        if propertyName in dnProps:
            return True
        
        globProps = self.props('global-properties')
        if propertyName in globProps:
            return True
        else:
            return False


    def hasLocatorProperty(self, processName, propertyName, host = None):
        pProps = self.processProps(processName, host = host)
        if propertyName in pProps:
            return True
        
        hostProps = self.hostProps(host = host)
        if propertyName in hostProps:
            return True
        
        lProps = self.props('locator-properties')
        if propertyName in lProps:
            return True
        
        globProps = self.props('global-properties')
        if propertyName in globProps:
            return True
        else:
            return False

        

    def gfshArgs(self, processType, processName):
        temp = dict()
        for source in [self.props('global-properties'),
                       self.props(processType + '-properties'),
                       self.hostProps(),
                       self.processProps(processName)]:
            for k in source.keys():
                temp[k] = source[k]
                
        result = self.buildGfshArgs(temp)
        
        if 'jvm-options' in temp:
            for option in temp['jvm-options']:
                result.append('--J={0}'.format(option))

        return result
        
        
        
    
    
