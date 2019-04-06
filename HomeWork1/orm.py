import mysql.connector
from mysql.connector import Error
import itertools


conn = mysql.connector.connect(user ='root',
        password ='root',
        host ='localhost',
        database ='mysqldb')



class Field:
    def __init__(self, f_type, required=True, default=None):
        self.f_type = f_type
        self.required = required
        self.default = default

    def validate(self, value):
        if value is None and not self.required:
            return None
        if not isinstance(value, (self.f_type, type(None))):
            raise ValueError('ValueError')
        return self.f_type(value)


class IntField(Field):
    def __init__(self, required=True, default=None):
        super().__init__(int, required, default)


class StringField(Field):
    def __init__(self, required=True, default=None):
        super().__init__(str, required, default)





class ModelMeta(type):
    def __new__(mcs, name, bases, namespace):
        if name == 'Model':
            return super().__new__(mcs, name, bases, namespace)

        meta = namespace.get('Meta')
        if meta is None:
            raise ValueError('meta is none')
        if not hasattr(meta, 'table_name'):
            raise ValueError('table_name is empty')

        # mro
        namespace['_fields'] = {}
        if bases[0] != Model:
            for base in bases:
                for key, field in base._fields.items():
                    namespace['_fields'][key] = field
                
        fields = {k: v for k, v in namespace.items()
                    if isinstance(v, Field)}
        for key, field in fields.items():
            namespace['_fields'][key] = field  

        namespace['_table_name'] = meta.table_name
        return super().__new__(mcs, name, bases, namespace)





class Manage:
    def __init__(self):
        self.model_cls = None

    def __get__(self, instance, owner):
        if self.model_cls is None:
            self.model_cls = owner
            self.table_name = owner._table_name
            self.fields = owner._fields
        return self

    def input_value(self, input_dict):
        result = {}
        for input_key, input_value in input_dict.items():
            if input_key not in self.fields:
                raise ValueError ('Incorrect key')
            input_value = self.fields[input_key].validate(input_value)
            if type(input_value) == str:
                result[input_key] = input_value             
            else: 
                result[input_key] = str(input_value)
        return result


    def create(self, **kwargs):
        cursor = conn.cursor()
        result = self.input_value(kwargs)

        query = (
            "INSERT INTO {table_name} ({result_key}) VALUES (%s)".format(
                table_name = self.table_name,
                result_key = ' '.join(result.keys())
            ))
        args = []
        for key, value in result.items():
            args.append(value)
        cursor.execute(query, args)

        conn.commit()
        cursor.close()


    def read(self):
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM {table_name}".format(table_name = self.table_name))
        return cursor.fetchall()
        cursor.close()


    def delete(self, **kwargs):
        cursor = conn.cursor()
        result = self.input_value(kwargs)
       
        query = (
           "DELETE FROM {table_name} WHERE {result_key} = %s".format(
               table_name = self.table_name,
               result_key = ' '.join(result.keys())
           ))
        args = []
        for key, value in result.items():
            args.append(value)
        cursor.execute(query, args)
    
        conn.commit()
        cursor.close()
        
    def update(self, **kwargs):
        cursor = conn.cursor()
        result = self.input_value(kwargs)
            
        if len(result) == 2:
            n = len(result) // 2          
            i = iter(result.items())      
            d1 = dict(itertools.islice(i, n))   
            d2 = dict(i)
            
            query = ("UPDATE {table_name} SET {d1_key} = %s WHERE {d2_key} = %s".format(
                table_name = self.table_name,
                d1_key = ' '.join(d1.keys()),
                d2_key = ' '.join(d2.keys()),
            ))
            args = []
            for key, value in d1.items():
                args.append(value)
            for key, value in d2.items():
                args.append(value)

            cursor.execute(query, args)
            
        else: 
            query = ("UPDATE {table_name} SET {result_key} = %s".format(
                table_name = self.table_name,
                result_key = ' '.join(result.keys()),
            ))
            args = []
            for key, value in result.items():
                args.append(value)
            cursor.execute(query, args)   

        conn.commit()
        cursor.close()





class Model(metaclass=ModelMeta):
    class Meta:
        table_name = ''

    objects = Manage()
    # todo DoesNotExist

    def __init__(self, *_, **kwargs):
        for field_name, field in self._fields.items():
            value = field.validate(kwargs.get(field_name))
            setattr(self, field_name, value)


class User(Model):
    id = IntField()
    name = StringField()

    class Meta:
        table_name = 'user'


class Man(User):
    sex = StringField()

    class Meta:
        table_name = 'user1'

# user = User(id=1, name='name')
# User.objects.create(id=1, name='name')


# User.objects.update(id=1)  

# User.objects.delete(id=16)

# User.objects.delete(name='Pasha')


# User.objects.filter(id=2).filter(name='petya')

# user.name = '2'
# user.save()
# user.delete()

# User.objects.update(name = 'Dasha', id=15)
# User.objects.update(name = 'Igor')
# User.objects.create(name = 'Petya')
print(User.objects.read())
# Man.objects.read()

