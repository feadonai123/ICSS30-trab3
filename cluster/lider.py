import math
import random
import threading
from datetime import date
import time
import Pyro5.api

random.seed(0)

class Lider:

  def __init__(self, name_server, num_quorum):
    self.estado = 'LIDER'
    self.id = random.randint(0, 1000)
    self.name_server = name_server
    self.num_quorum = num_quorum
    self.log = []
    self.brokers = []
    
    self.start_check_brokers()
    
  def add_broker(self, id, ref, estado):
    self.brokers.append((id, ref, estado, time.time()))
    print("Broker {0} enrolled".format(id))
    print("Len brokers: {0}".format(len(self.brokers)))
    
  def add_log(self, log):
    self.log.append(log)
    print("Log added")
    print("Len log: {0}".format(len(self.log)))
    
    brokers_success = 0
    can_commit = False
    
    if len(self.brokers) == 0:
      can_commit = True
      
    for broker in self.brokers:
      id, ref, estado, lastBeatAt = broker
      print("connecting to middleware {0}".format(ref))
      middleware = MiddlewareRequest(connection_str=ref, connection_type="URI")
      if middleware.connect() is False:
        print("Error connecting to middleware")
        raise Exception("Error connecting to middleware")
      else:
        print("Connected to middleware {0}".format(id))

      print("notify_added_log")
      res = middleware.notify_added_log()
      if res == "NOK":
        print("Error adding log")
      else:
        print("Log added with success")
        print("thinking about quorum")
        brokers_success += 1
      
      if brokers_success >= self.num_quorum:
        print("quorum reached")
        can_commit = True
        break
    
    if can_commit is True:
      print("commiting")
      print("New Log: {0}".format(self.log))
      return True
    else:
      print("aborting")
      self.log = self.log[:-1]
      print("log: {0}".format(self.log))
      return False
    
  def search(self, offset):
    print("Searching")
    print("Offset: {0}".format(offset))
    print("Len log: {0}".format(len(self.log)))
    print("Log: {0}".format(self.log[offset:]))
    
    if(offset == 0 and len(self.log) == 0):
      print("returning", "OK", [])
      return ('OK', [])
    elif(offset < len(self.log)):
      print("returning", self.log[offset:])
      return ('OK', self.log[offset:])
    else:
      print("returning", "NOK")
      return ("NOK", len(self.log) - 1)
  
  def set_broker_alive(self, broker_id):
    index = 0
    activeBrokers = 0
    for broker in self.brokers:
      id, ref, estado, lastBeatAt = broker
      if(id == broker_id):
        self.brokers[index] = (id, ref, estado, time.time())
        activeBrokers = activeBrokers + 1
        break
      index = index + 1
      
  def start_check_brokers(self):
    def check_brokers_loop():
      while True:
        print("len brokers", len(self.brokers))
        index = 0
        activeBrokers = 0
        for broker in self.brokers:
          id, ref, estado, lastBeatAt = broker
          if(time.time() - lastBeatAt > 10):
            print("Broker id {0} está desativo".format(id))
          else:
            activeBrokers = activeBrokers + 1
          index = index + 1
          
        if(activeBrokers < self.num_quorum):
          print("Quorum abaixo do permitido")
          for broker in self.brokers:
            id, ref, estado, lastBeatAt = broker
            if(estado == "OBSERVADOR"):
              print("Promovendo broker {0} para votante".format(id))
              
              middleware = MiddlewareRequest(connection_str=ref, connection_type="URI")
              if middleware.connect() is False:
                print("Error connecting to middleware")
                raise Exception("Error connecting to middleware")
              else:
                print("Connected to middleware {0}".format(id))

              res = middleware.notify_promote_to_votante()
              if res == "NOK":
                print("Error promoting broker")
              else:
                print("Broker promoted with success")
              
              break
            
        
        time.sleep(5)
   
    thread = threading.Thread(target=check_brokers_loop)
    thread.start()
     
  def read_log(self, offset):
    offset = int(offset)
    print("Reading log", offset)
    if offset < len(self.log):
      print("returning", self.log[offset:])
      return (True, self.log[offset:])
    else:
      print("returning", (False, None))
      return (False, None)
      
  def check_response(self, res):
    return res == "OK"

class MiddlewareListen(object):
  
  @Pyro5.server.expose
  def enroll_broker(self, id, ref, estado):
    try:
      print("Enrolling broker {0}".format(id))
      print("Ref: {0}".format(ref))
      lider.add_broker(id, ref, estado)
      return "OK"
    except Exception as e:
      print("e", e)
      return False
  
  @Pyro5.server.expose
  def write(self, log):
    print("Writing log")
    success = lider.add_log(log)
    if success is True:
      return "OK"
    else:
      return "NOK"
    
  @Pyro5.server.expose
  def read(self, offset):
    success, data = lider.read_log(offset)
    if success is True:
      return data
    else:
      return "NOK"
  
  @Pyro5.server.expose
  def search(self, offset):
    return lider.search(offset)
  
  @Pyro5.server.expose
  @Pyro5.api.oneway
  def heartbeat(self, id):
    print("Heartbeat from", id)
    lider.set_broker_alive(id)
    return True
  
  def listen(self, server_name = None):
    try:
      daemon = Pyro5.server.Daemon()
      self.uri = daemon.register(self)
      if server_name is not None:
        ns = Pyro5.api.locate_ns()
        ns.register(server_name, self.uri)
        
      def requestLoop():
        print("Listening at {0}".format(self.uri))
        daemon.requestLoop()
      
      thread = threading.Thread(target=requestLoop)
      thread.start()
      return True
    except Exception as e:
      return False
  
class MiddlewareRequest(object):
  
  def __init__(self, connection_str, connection_type="NS"):
    if connection_type not in ["NS", "URI"]:
      raise Exception("Invalid connection type")
    if connection_type == "NS":
      self.server_name = connection_str
    else:
      self.uri = connection_str
      
  def connect(self):
    try:
      if self.uri is None:
        ns = Pyro5.api.locate_ns()
        uri = ns.lookup(self.server_name)
      else:
        uri = self.uri
      print("trying to connect", uri)
      self.proxy = Pyro5.api.Proxy(uri)
      print("connected")
      return True
    except:
      print("error connecting")
      return False
    
  def notify_added_log(self):
    try:
      return self.proxy.added_log()
    except Exception as e:
      return "NOK"
    
  def notify_promote_to_votante(self):
    try:
      return self.proxy.promote_to_votante()
    except Exception as e:
      return "NOK"


name_server = "LIDER-EPOCA1"
lider = Lider(name_server=name_server, num_quorum=2)
MiddlewareListen().listen(name_server)

# daemon = Pyro5.server.Daemon()
# ns = Pyro5.api.locate_ns()
# uri = daemon.register(Middleware)
# ns.register(lider.name_server, uri)

# print("Ready.")
# daemon.requestLoop()