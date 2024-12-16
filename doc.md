# Iniciar server name
python -m Pyro5.nameserver

# Listar URIs
python -m Pyro5.nsc list

# Correções realizadas 16/12
- Adicionado log_uncommitted no lider. Dessa forma, ao receber uma solicitação de READ, será retornado sempre o log comitado
- Adicionado rollback nos consumidores. Dessa forma, quando o lider abortar uma solicitação de WRITE de um novo log, todos os consumidores verificam novamente seus logs, a fim de não possuirem dados inválidos