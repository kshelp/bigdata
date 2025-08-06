from pyspark.sql import Row

Person = Row('name', 'age', 'family')
person1 = Person('joseph', 35)
person2 = Person('jina', 30)
person3 = Person('julian', 15)

rows = [person1, person2, person3]

print(rows)
