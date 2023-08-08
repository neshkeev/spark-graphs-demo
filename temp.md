### Обобщение императивного алгоритма

Поставив все три реализованных алгоритма друг на против друга можно заметить, что остается неизменным, а какие части меняются:

![](https://habrastorage.org/webt/n6/2m/do/n62mdoxhoefgafnbebpvymfqevy.png)

![](https://habrastorage.org/webt/5y/bk/av/5ybkavuranpr2ebfmmjxpt7jmfc.png)

![](https://habrastorage.org/webt/v1/1v/fo/v11vfoxqx5yqjmijix0elvvczrq.png)

1. Тип ребер: графы могут быть взвешенные и нет. Невзвешенные графы можно сделать взвешанными, если указать пустой вес, например `0`. Следовательно, тип ребер может быть один везде: `Tuple[PregelVertex, PregelVertex, int]`;
1. Инициализация:
    - каждая вершина имеет идентификатор,
    - инициализация хранимого в вершине значение управляется программистом, поэтому можно вынести параметр конструктора в виде лямбда функции: `Callable[[], int]`.
1. Каждый раз когда итоговое сообщение вычисляется, оно сразу сравнивается с текущим значением в вершине и при наступлении определенного условия значение вершины обновляется. Можно вынести вычисление итогового сообщения внутрь функции, которая будет принимать список сообщений и текущее значение вершины и возвращать новое значение вершины: `Callable[[List[int], int], int]`;
1. Отправляемое сообщение конструируется при помощи текущего значения и исходящего ребра, вычисление сообщения можно вынести в лямбда-функцию, которая принимает текущее значение вершины и ребро, а возвращает сообщение: `Callable[[int, Tuple[PregelVertex, PregelVertex, int]], int]`.

Учитывая изложенные наблюдения, можно преобразовать класс PregelVertex следующим образом:

```python
class Edge(NamedTuple[PregelVertex, PregelVertex, int]]):
    src: PregelVertex
    dst: PregelVertex
    weight: int

class PregelVertex:

    def __init__(
        self, id: int,
        value: Callable[[], int],
        value_updater: Callable[[List[int], int], int],
        message_constructor: Callable[[int, Edge, int]
    ):
        self.id = id
        self.value = value()
        self.value_updater = value_updater
        self.message_constructor = message_constructor

    def compute(self, messages: List[MessageValue]) -> None:
        new_value = self.value_updater(messages, self.value)
        if self.value != new_value:
            self.value = new_value

            for edge in self.out_edges():
                target = edge.dst
                message = self.message_constructor(self.value, e)
                self.send_message(target.id, message)
```

### Пример "Связные компоненты"

```python
vertices = get_vertices()

сс_pregel_vertices = [
    PregelVertex(
        id=v,
        value=lambda: v,
        value_updater=lambda messages, value: return min(min(messages), value),
        message_constructor=lambda value, _edge: value
    )
    for v in vertices
]
```

### Пример "Крачайший путь"

```python
vertices = get_vertices()

start_vertex = 1

сс_pregel_vertices = [
    PregelVertex(
        id=v,
        value=lambda: 0 if v == start_vertex else sys.maxsize // 2,
        value_updater=lambda messages, value: return min(min(messages), value),
        message_constructor=lambda value, edge: value + edge.weight
    )
    for v in vertices
]
```

### Пример "Топологическая сортировка"

```python
vertices = get_vertices()

сс_pregel_vertices = [
    PregelVertex(
        id=v,
        value=lambda: 1,
        value_updater=lambda messages, value: return max(max(messages) + 1, value),
        message_constructor=lambda value, edge: value
    )
    for v in vertices
]
```