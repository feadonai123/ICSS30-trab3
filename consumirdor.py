import Pyro5.api

offset = input("Informe o OFFSET? ").strip()

ns = Pyro5.api.locate_ns()
uri = ns.lookup("LIDER-EPOCA1")
proxy = Pyro5.api.Proxy(uri)

res = proxy.read(offset)
print(res)