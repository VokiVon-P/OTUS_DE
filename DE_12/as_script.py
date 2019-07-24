
import aerospike
from aerospike import exception as ex
from aerospike import predicates as p
import sys

# Configure the client
config = {
    'hosts': [ ('172.17.0.2', 3000) ]
}

_NS = 'test'
_SET = 'LFTVAL'

# глобальный клиент для аероспайка
_CLIENT = None


### служебное - Инициализация соединения с базой
def connect_aerospike():
    if _CLIENT is not None and _CLIENT.is_connected():
        return _CLIENT

    """ 
    # Optionally set policies for various method types
    write_policies = {'total_timeout': 2000, 'max_retries': 0}
    read_policies = {'total_timeout': 1500, 'max_retries': 1}
    policies = {'write': write_policies, 'read': read_policies}
                
    """
    policies = {'key': aerospike.POLICY_KEY_SEND} # добавлено для отладки - в бою ключи можно не пересылать 

    config['policies'] = policies
    
    # Create a client and connect it to the cluster
    try:
        CLIENT = aerospike.client(config).connect()
        return CLIENT
    except ex.ClientError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        sys.exit(1)


### служебное - закрываем соединение с базой 
def disconnect_aerospike():
      if _CLIENT is not None and _CLIENT.is_connected():
        _CLIENT.close()


### добавляем запись
def add_customer(customer_id, phone_number, lifetime_value):
    key = (_NS, _SET, str(customer_id))
    bins = {
        'phone_number': phone_number,
        'lifetime_value': lifetime_value,
    }
    try:
        client = connect_aerospike()
        client.put(key, bins, meta={'ttl':60})
    except ex.ClientError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        client.close()
        sys.exit(1)

### читаем значение LTV по id 
def get_ltv_by_id(customer_id):
    key = (_NS, _SET, str(customer_id))

    try:
        client = connect_aerospike()
        (k, m, bins) = client.get(key)
        return bins.get('lifetime_value')
        
    except ex.RecordNotFound:
        return f"Нет значения 'lifetime_value' для ключа {customer_id}"

    except Exception as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        client.close()
        sys.exit(1)


### читаем LTV по полю телефона, если делаем это первый раз, то создается индекс
def get_ltv_by_phone(phone_number):
    
    try:
        bins = 'phone_number';
        client = connect_aerospike()
        q = client.query(_NS, _SET)
        q.select('lifetime_value')
        q.where(p.equals(bins, phone_number))
        records = q.results( {'total_timeout':2000} )
        if len(records) > 0:
            (k, m, bins) = records[0]
            return bins.get('lifetime_value')
        else:
            return f"Нет значения 'lifetime_value' для поля {phone_number}"

    except ex.IndexNotFound:
        # создаем индекс и пробуем снова (вынести в отдельную функцию создание индекса!)
        client.index_string_create(_NS, _SET, bins, 'index_'+bins)
        return get_ltv_by_phone(phone_number)

    except Exception as e:
        print(f"Error: {e}")
        client.close()
        sys.exit(1)

# проверочная часть

_CLIENT = connect_aerospike()

# создаем записи
for id in range(0, 1001):
    add_customer(id, f'+{7000000000 + id}', 1000 + id)    

# создаем читаем по id 
for id in range(0, 1001, 50):
    print(f"LFT value = {get_ltv_by_id(id)} for Id = {id}")

# читаем не существующий id
print(f"LFT value = {get_ltv_by_id(2000)} for Id = {id}")

# читаем по телефону - последний не существует
for id in range(0, 1001, 100):
    nphone = "+"+str(7000000000 + id + 10)
    print(f"LFT value = {get_ltv_by_phone(nphone)} for phone = {nphone}")

# закрываем базу
disconnect_aerospike()

    
    
