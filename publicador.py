import Pyro5.api

name = input("What is your message? ").strip()

ns = Pyro5.api.locate_ns()
uri = ns.lookup("LIDER-EPOCA1")
proxy = Pyro5.api.Proxy(uri)

print(proxy.write(name))