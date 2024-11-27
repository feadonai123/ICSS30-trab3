import random
from time import sleep
import Pyro5.api
import threading

# random.seed(1)

class Observador():
  
  def __init__(self, lider_server_name, num_quorum):
    self.estado = 'OBSERVADOR'
    self.id = random.randint(0, 1000)
    self.lider_server_name = lider_server_name
    self.num_quorum = num_quorum
    self.log = []
    
    if self.listen() is False:
      raise Exception("Error listening")
    else:
      print("Listening")      
      
    if self.enroll_lider() is False:
      raise Exception("Error enrolling lider")
    else:
      print("Enrolled to lider")
      
    self.start_heartbeat()
    
  def enroll_lider(self):
    middleware = MiddlewareRequest(connection_str=self.lider_server_name, connection_type="NS")
    if middleware.connect() is False:
      raise Exception("Error connecting to middleware")
    else:
      print("Connected to middleware")
      
    res = middleware.enroll_broker(self.id, self.uri, self.estado)
    return self.check_response(res)
  
  def update_log(self):
    print("Adding log")
    middleware = MiddlewareRequest(connection_str=self.lider_server_name, connection_type="NS")
    if middleware.connect() is False:
      raise Exception("Error connecting to middleware")
    else:
      print("Connected to middleware")
      
    res_success, res_data = middleware.search(len(self.log))
    if res_success == "NOK":
      print("Error searching log")
      print("log: {0}".format(self.log))
      # realizar tratativa. Deve receber offset do lider, fazer nova busca, truncar log atual
      self.log = self.log[:res_data]
      
      res_success2, res_data2 = middleware.search(res_data)
      if res_success2 == "NOK":
        print("Error searching log")
        print("log: {0}".format(self.log))
      else:
        self.log.extend(res_data2)
        print("New Log: {0}".format(self.log))

      return False
    else:
      self.log.extend(res_data)
      print("Log added")
      print("New Log: {0}".format(self.log))
      return True  
  
  def promoteToVotante(self):
    self.estado = "VOTANTE"
    self.update_log()
    print("Promoted to Votante")
    
  def listen(self):
    middleware = MiddlewareListen()
    res = middleware.listen()
    self.uri = middleware.uri
    return res
  
  def check_response(self, res):
    return res == "OK"
  
  def start_heartbeat(self):
    def heartbeat_loop():
      while True:
        if(self.estado == "VOTANTE"):
          middleware = MiddlewareRequest(connection_str=self.lider_server_name, connection_type="NS")
          if middleware.connect() is False:
            raise Exception("Error connecting to middleware")
            
          middleware.heartbeat(self.id)
        sleep(3)

    thread = threading.Thread(target=heartbeat_loop)
    thread.start()
    
    
class MiddlewareRequest(object):
  
  def __init__(self, connection_str, connection_type="NS"):
    if connection_type not in ["NS", "URI"]:
      raise Exception("Invalid connection type")
    if connection_type == "NS":
      self.server_name = connection_str
      self.uri = None
    else:
      self.uri = connection_str
      self.server_name = None
    
  def connect(self):
    try:
      if self.server_name is not None:
        ns = Pyro5.api.locate_ns()
        uri = ns.lookup(self.server_name)
      self.proxy = Pyro5.api.Proxy(uri)
      return True
    except:
      return False
    
  def enroll_broker(self, id, ref, estado):
    try:
      return self.proxy.enroll_broker(id, ref, estado)
    except Exception as e:
      print(e)
      return "NOK"
  
  def search(self, offset):
    try:
      print("Searching", offset)
      return self.proxy.search(offset)
    except Exception as e:
      print(e)
      return "NOK"
    
  def heartbeat(self, id):
    try:
      return self.proxy.heartbeat(id)
    except Exception as e:
      print(e)
      return "NOK"
  
class MiddlewareListen(object):
  @Pyro5.api.callback
  @Pyro5.server.expose
  def added_log(self):
    if(observador.estado == "VOTANTE"):
      print("Log added")
      return observador.update_log()
    else:
      return "NOK"
      
      
  @Pyro5.api.callback
  @Pyro5.server.expose
  def promote_to_votante(self):
    observador.promoteToVotante()
    return "OK"
  
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
  
observador = Observador(lider_server_name="LIDER-EPOCA1", num_quorum=2)
