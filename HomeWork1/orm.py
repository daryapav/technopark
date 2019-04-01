import mysql.connector
from mysql.connector import Error
import itertools


default_config = {
 'user': 'root',
 'password': 'root',
 'host': 'localhost',
 'database': 'mysqldb'
 }

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

        # todo mro     //я сделаю

        fields = {k: v for k, v in namespace.items()
                  if isinstance(v, Field)}
        namespace['_fields'] = fields
        namespace['_table_name'] = meta.table_name
        return super().__new__(mcs, name, bases, namespace)




# def  map_fields_type(t):
#     if t == int:
#         return 'INT'
#     if t == str:
#         return 'TEXT'


# def make_fields_stmt(meta_fields):
#     fields = []
#     for k, v in meta_fields.items():
#         # f_type = map_fields_type(v)
#         fields.append('{f_name} {f_type}'.format(f_name = k, f_type = v))
#     # print(', '.join(fields))
#     return ', '.join(fields)




class Manage:
    def __init__(self):
        self.model_cls = None

    def __get__(self, instance, owner):
        if self.model_cls is None:
            self.model_cls = owner
            self.table_name = owner._table_name
            self.fields = owner._fields
        return self

    def create(self, **kwargs):
        try:
            conn = mysql.connector.connect(**default_config)
            cursor = conn.cursor()
            fields = {}
            for fields_key, fields_value in kwargs.items():
                if type(fields_value) == str:
                    fields[fields_key] = "'" + fields_value + "'" 
                elif type(fields_value) == int:
                    fields[fields_key] = str(fields_value)
                else:
                    raise ValueError ('Incorrect type')

            cursor.execute('INSERT INTO {table_name} ({fields_key}) VALUES ({fields_value}) '.format(
                    table_name = self.table_name, 
                    fields_key = ' '.join(fields.keys()),
                    fields_value = ' '.join(fields.values())))
            conn.commit()

        except Error as e:
            print(e)

        finally:
            cursor.close()
            conn.close()

    def read(self):
        try:
            conn = mysql.connector.connect(**default_config)
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM {table_name}".format(table_name = self.table_name))

            row = cursor.fetchone()

            while row is not None:
                print(row)
                row = cursor.fetchone()

        except Error as e:
            print(e)

        finally:
            cursor.close()
            conn.close()
    
    def delete(self, **kwargs):
        try:
            conn = mysql.connector.connect(**default_config)
            cursor = conn.cursor()
            fields = {}

            for fields_key, fields_value in kwargs.items():                   # возможно, это стоит вынести в отдельную функцию 
                if type(fields_value) == str:                            
                    fields[fields_key] = "'" + fields_value + "'" 
                elif type(fields_value) == int:
                    fields[fields_key] = str(fields_value)
                else:
                    raise ValueError ('Incorrect type')
                    
            cursor.execute('DELETE FROM {table_name} WHERE {fields_key} = {fields_value}'.format(
                table_name = self.table_name,
                fields_key = ' '.join(fields.keys()),
                fields_value = ' '.join(fields.values())
            ))
            conn.commit()

        except Error as e:
            print(e)

        finally:
            cursor.close()
            conn.close()
        
    def update(self, **kwargs):
        try:
            conn = mysql.connector.connect(**default_config)
            cursor = conn.cursor()
            fields = {}
            for fields_key, fields_value in kwargs.items():
                if type(fields_value) == str:
                    fields[fields_key] = "'" + fields_value + "'" 
                elif type(fields_value) == int:
                    fields[fields_key] = str(fields_value)
                else:
                    raise ValueError ('Incorrect type')
            
            if len(fields) == 2:
                n = len(fields) // 2          
                i = iter(fields.items())      
                d1 = dict(itertools.islice(i, n))   
                d2 = dict(i)

                cursor.execute('UPDATE {table_name} SET {d1_key} = {d1_value} WHERE {d2_key} = {d2_value}'.format(
                    table_name = self.table_name,
                    d1_key = ' '.join(d1.keys()),
                    d1_value = ' '.join(d1.values()),
                    d2_key = ' '.join(d2.keys()),
                    d2_value = ' '.join(d2.values())
                ))
            else:           #len(fields) == 1
                cursor.execute('UPDATE {table_name} SET {fields_key} = {fields_value}'.format(
                    table_name = self.table_name,
                    fields_key = ' '.join(fields.keys()),
                    fields_value = ' '.join(fields.values())
                ))
        

            conn.commit()

        except Error as e:
            print(e)
            
        finally:
            cursor.close()
            conn.close()





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


# class Man(User):
#     sex = StringField()


# user = User(id=1, name='name')
# User.objects.create(id=1, name='name')


# User.objects.update(id=1)  


# User.objects.delete(id=5)

# User.objects.delete(name='Pasha')
# User.objects.delete (name = [1,2,3])

# User.objects.filter(id=2).filter(name='petya')

# user.name = '2'
# user.save()
# user.delete()

# User.objects.update(name = 'Dasha', id=9)
# User.objects.update(name = 'Jenya')
User.objects.read()
# User.objects.create(name = 'Alica')