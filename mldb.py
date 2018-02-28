import playhouse.postgres_ext as peewee

db = peewee.PostgresqlExtDatabase(None)


class BaseModel(peewee.Model):
    """
    
    """
    
    class Meta:
        database = db


class Dataset(BaseModel):
    """
    
    """
    
    id = peewee.PrimaryKeyField()
    
    name = peewee.CharField(unique=True, null=False, index=True)
    metadata = peewee.JSONField(null=True)
    
    def __getitem__(self, item):
        """
        
        :param item:
        :return:
        """
        
        query = Node.select().where(Node.name == item)
        if not query.exists():
            raise ValueError
        
        return query.get
    
    def __repr__(self):
        """
        
        :return:
        """
        
        return "<{} name={} metadata={}>".format(
            self.__class__.__name__,
            self.name,
            self.metadata
        )
    
    class Meta:
        db_table = 'datasets'
        
        order_by = (
            'name',
        )


class Node(BaseModel):
    id = peewee.PrimaryKeyField()
    
    dataset = peewee.ForeignKeyField(Dataset, related_name='nodes', on_delete='cascade')
    name = peewee.CharField(unique=False, null=False, index=True)
    
    func = peewee.CharField(null=False, unique=False)
    kwargs = peewee.JSONField(null=False, unique=False)
    metadata = peewee.JSONField(null=True, unique=False)
    
    backend = peewee.CharField(null=False, default=True)
    
    @property
    def sources(self):
        """
        
        :return:
        """
        
        return Edges.select().where(Edges.sink == self)
    
    @property
    def children(self):
        """
        
        :return:
        """
        
        raise NotImplementedError
    
    def __repr__(self):
        """
        
        :return:
        """
        
        return '<{} dataset={} name={}>'.format(
            self.__class__.__name__,
            self.dataset.name,
            self.name
        )
    
    class Meta:
        db_table = 'nodes'
        
        order_by = (
            'dataset',
            'name',
        )
        
        indexes = (
            (('dataset', 'name',), True),
        )


class Edges(BaseModel):
    id = peewee.PrimaryKeyField()
    
    sink = peewee.ForeignKeyField(Node, related_name='sink', on_delete='cascade')
    source = peewee.ForeignKeyField(Node, related_name='source', on_delete='cascade')
    
    def __repr__(self):
        """
        
        :return:
        """
        
        return '<{} sink={} source={}>'.format(
            self.__class__.__name__,
            self.sink.name,
            self.source.name
        )
    
    class Meta:
        db_table = 'edges'
        
        order_by = (
            'sink',
            'source',
        )
        
        indexes = (
            (('sink', 'source',), True),
        )


class Data(BaseModel):
    id = peewee.PrimaryKeyField()
    
    node = peewee.ForeignKeyField(
        Node,
        related_name='data',
        on_delete='cascade',
        null=False,
        index=True,
    )
    
    d = peewee.BinaryJSONField(null=False)
    
    def __repr__(self):
        """
        
        :return:
        """
        
        return '<{} d={}>'.format(
            self.__class__.__name__,
            self.d
        )
    
    class Meta:
        db_table = 'data'
        
        order_by = (
            'node',
        )

"""

"""

all_tables = [
    Dataset,
    Node,
    Edges,
    Data,
]

# drop table data; drop table sources; drop table edges; drop table nodes; drop table datasets ;
