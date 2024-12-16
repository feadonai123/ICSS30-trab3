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
  
  def update_log(self, getUncommitted=False):
    print("Adding log")
    middleware = MiddlewareRequest(connection_str=self.lider_server_name, connection_type="NS")
    if middleware.connect() is False:
      raise Exception("Error connecting to middleware")
    else:
      print("Connected to middleware")
      
    res_success, res_data = middleware.search(len(self.log), getUncommitted)
    print("res_data: {0}".format(res_data))
    if res_success == "NOK":
      print("Error searching log")
      print("log: {0}".format(self.log))
      self.log = self.log[:res_data]
      
      res_success2, res_data2 = middleware.search(res_data, getUncommitted)
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
      # print(f"\n[INICIO] Tentando conectar com {uri}")
      self.proxy = Pyro5.api.Proxy(uri)
      # print("[FIM] Conectado com sucesso")
      return True
    except:
      print("[FIM] Falha ao se conectar")
      return False
    
  def enroll_broker(self, id, ref, estado):
    try:
      print("\n[INICIO] Requisitando enroll_broker", id, ref, estado)
      res =  self.proxy.enroll_broker(id, ref, estado)
      print("[FIM] Retorno enroll_broker SUCESSO: ", res)
      return res
    except Exception as e:
      print("[FIM] Retorno enroll_broker ERRO: ", e)
      return "NOK"
  
  def search(self, offset, getUncommitted=False):
    try:
      print("\n[INICIO] Requisitando search", offset)
      return self.proxy.search(offset, getUncommitted)
    except Exception as e:
      print("[FIM] Retorno search ERRO: ", e)
      return "NOK"
    
  def heartbeat(self, id):
    try:
      # print("\n[INICIO] Requisitando heartbeat", id)
      # print("[FIM] Retorno heartbeat SUCESSO")
      return self.proxy.heartbeat(id)
    except Exception as e:
      # print("[FIM] Retorno heartbeat ERRO: ", e)
      return "NOK"
  
class MiddlewareListen(object):
  @Pyro5.api.callback
  @Pyro5.server.expose
  def added_log(self):
    print(f"\n[INICIO] Recebendo added_log")
    if(observador.estado == "VOTANTE"):
      res = observador.update_log(getUncommitted=True)
      print(f"[FIM] Retorno added_log", res)
      return res
    else:
      print(f"[FIM] Retorno added_log NOK")
      return "NOK"
      
  @Pyro5.api.callback
  @Pyro5.server.expose
  def rollback(self, offset):
    print(f"\n[INICIO] Recebendo rollback", offset)
    res = observador.update_log(getUncommitted=False)
    print(f"[FIM] Retorno rollback", res)
    return res
  
  @Pyro5.api.callback
  @Pyro5.server.expose
  def promote_to_votante(self):
    print(f"\n[INICIO] Recebendo promote_to_votante")
    observador.promoteToVotante()
    print(f"[FIM] Retorno promote_to_votante OK")
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
