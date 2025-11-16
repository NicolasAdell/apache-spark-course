from pyspark import SparkConf, SparkContext


startCharacterID = 536
targetCharacterID = 13

conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf = conf)

hitCounter = sc.accumulator(0)

def main():
    iterationRdd = createStartingRdd()

    for it in range(0, 10):
        print(f"Running BFS iteration #{it+1}")

        mapped = iterationRdd.flatMap(bfsMap)

        print(f"Processing {mapped.count()} values.")

        if hitCounter.value > 0:
            print(f"Hit the target character! From {hitCounter.value} different directions")
            break
        
        iterationRdd = mapped.reduceByKey(bfsReduce)


def bfsMap(node):
    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    # The node needs to be processed
    if color == 'GRAY':
        for connection in connections:
            newCharacterID = connection
            newDistance = distance + 1
            newColor = 'GRAY'

            if targetCharacterID == connection:
                hitCounter.add(1)

            newEntry = (newCharacterID, ([], newDistance, newColor))
            results.append(newEntry)

        color = 'BLACK'

    results.append((characterID, (connections, distance, color)))
    return results


def bfsReduce(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    distance = 9999
    color = 'WHITE'
    edges = []
    
    # See if one is the original node
    if len(edges1) > 0:
        edges = edges1
    elif len(edges2) > 0:
        edges = edges2

    # Preserve minimum distance
    if distance1 < distance:
        distance = distance1

    if distance2 < distance:
        distance = distance2

    # Keep darkest color
    if color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK'):
        color = color2

    if color2 == 'GRAY' or color2 == 'BLACK':
        color = color2
    
    return (edges, distance, color)


def convertToBFS(line):
    fields = line.split()
    heroID = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))

    color = 'WHITE'
    distance = 9999

    if (heroID == startCharacterID):
        color = 'GRAY'
        distance = 0
    
    return heroID, (connections, distance, color)


def createStartingRdd():
    inputFile = sc.textFile("file:///SparkCourse/datasets/Marvel+Graph")
    return inputFile.map(convertToBFS)


if __name__ == "__main__":
    main()