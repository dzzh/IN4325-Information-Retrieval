import math

__author__ = 'zmicier'

clusters = list()
classes = list()

clusters.append([1,1,1,1,1,1,1,1,1,1,2,2])
clusters.append([1,1,2,2,2,2,2,2,2,2,3,3])
clusters.append([1,1,1,1,3,3,3,3,3,3])
classes.append(16)
classes.append(10)
classes.append(8)
elements = 34

#clusters.append([1,1,1,1,1,2,2])
#clusters.append([1,2,2,2,2,3])
#clusters.append([1,1,3,3,3])
#classes.append(8)
#classes.append(5)
#classes.append(4)
#elements = 17

def P(cluster, doc_class):
    return cluster.count(doc_class)/elements

def I():
    i = 0
    print()
    for w in range(0,len(clusters)):
        for c in range(0, len(classes)):
            log = P(clusters[w],c + 1)/(len(clusters[w])/elements * classes[c]/elements)
            if not log:
                log = 1
            i += P(clusters[w],c + 1) * math.log(log)
    return i

def Hw():
    h = 0
    for w in range(0, len(clusters)):
        h -= len(clusters[w])/elements * math.log(len(clusters[w])/elements)
    return h

def Hc():
    h = 0
    for c in range(0, len(classes)):
        h -= classes[c]/elements * math.log(classes[c]/elements)
    return h

def main():
    print("NMI:", I() / (math.fabs(Hw() + Hc()) / 2))

if __name__ == "__main__":
    main()



