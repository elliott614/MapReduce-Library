// Copyright 2019 Elliott Martinson
#include "./mapreduce.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

struct pair{
    char *key;
    char *value;
};

struct bucket{
    struct pair **pairs;  // array of pointers to pairs
    int last;             // (also works w/ array of pairs)
    int curr;  // only used by get_next()
    size_t pairssize;  // number of allocated indeces
    pthread_mutex_t array_lock;
};
// global variables
struct bucket *hashtable;
int kNumPartitions;
int kNumReducers;
int kNumMappers;
int kFilesPerMap;
Partitioner partitioner = MR_DefaultHashPartition;
Mapper mapper;
Reducer reducer;

char *get_next(char *key, int partition_number) {
    if (hashtable[partition_number].curr > hashtable[partition_number].last
          || strcmp(hashtable[partition_number].pairs
                   [hashtable[partition_number].curr]->key, key) != 0)
        return NULL;
    return hashtable[partition_number].pairs
              [hashtable[partition_number].curr++]->value;
}

void *map_thread(void *arg) {
    for (int i = 0; i < kFilesPerMap; i++)
        if (((char **) arg)[i])
            mapper(((char **) arg)[i]);
    pthread_exit(NULL);
}
// use partition_number % num_reducers = reducer_id assigned
void *reduce_thread(void *arg) {
    int *rid = (int *) arg;
    for (int k = *rid; k < kNumPartitions; k += kNumReducers)
        while (hashtable[k].curr <= hashtable[k].last) {
            char *key = hashtable[k].pairs[hashtable[k].curr]->key;
            reducer(key, get_next, k);
        }
    pthread_exit(NULL);
}

void MR_Run(int argc, char *argv[],
        Mapper map, int num_mappers,
        Reducer reduce, int num_reducers,
        Partitioner partition, int num_partitions) {
    // set global variables
    kNumPartitions = num_partitions;
    kNumReducers = num_reducers;
    kNumMappers = num_mappers;
    hashtable = (struct bucket *) malloc((num_partitions)
        * sizeof(struct bucket));
    if (partition)
        partitioner = partition;
    mapper = map;
    reducer = reduce;
    kFilesPerMap = (num_mappers + argc - 2) / num_mappers;  // cieling division
    // initialize hashtable
    for (int i = 0; i < num_partitions; i++) {
        hashtable[i].pairs = (struct pair **)
            malloc(sizeof(struct pair *));
        hashtable[i].last = -1;  // signals empty array
        hashtable[i].curr = 0;    // first call to get_next will be to element 0
        hashtable[i].pairssize = 1;
        pthread_mutex_init(&hashtable[i].array_lock, NULL);
    }
    // assign files to mappers
    char *files[kNumMappers][kFilesPerMap];
    for (int i = 0; i < argc - 1; i++)
        files[i % num_mappers][i / num_mappers] = argv[i + 1];
    for (int i = argc - 1; i < num_mappers * kFilesPerMap; i++)
        files[i % num_mappers][i / num_mappers] = NULL;
    // begin mapping
    pthread_t mappers[kNumMappers];
    for (int i = 0; i < num_mappers; i++)
        pthread_create(&mappers[i], NULL, map_thread, &files[i]);
    // join mapper threads
    for (int i = 0; i < num_mappers; i++)
        pthread_join(mappers[i], NULL);
    // sort partitions
    int compare(const void *a, const void *b) {
        char const *stringa = (*((struct pair **)a))->key;
        char const *stringb = (*((struct pair **)b))->key;
        return strcmp(stringa, stringb);
    }
    for (int i = 0; i < num_partitions; i++)
        qsort((struct pair *) hashtable[i].pairs, hashtable[i].last + 1,
                       sizeof(hashtable[i].pairs[0]), compare);
    // start reducing
    pthread_t reducers[kNumReducers];
    int rid[kNumReducers];
    for (int i = 0; i < num_reducers; i++) {
        rid[i] = i;
        pthread_create(&reducers[i], NULL, reduce_thread, &rid[i]);
    }
    // join reducer threads
    for (int i = 0; i < num_reducers; i++)
        pthread_join(reducers[i], NULL);
    // free hashtable
    for (int i = 0; i < num_partitions; i++) {
        for (int j = 0; j <= hashtable[i].last; j++) {
            free(hashtable[i].pairs[j]->key);
            free(hashtable[i].pairs[j]->value);
            free(hashtable[i].pairs[j]);
        }
        pthread_mutex_destroy(&hashtable[i].array_lock);
        free(hashtable[i].pairs);
    }
    free(hashtable);
}

void MR_Emit(char *key, char *value) {
    int bucket = partitioner(key, kNumPartitions);
    pthread_mutex_lock(&hashtable[bucket].array_lock);
    hashtable[bucket].pairs[++hashtable[bucket].last]
                        = malloc(sizeof(struct pair));
    hashtable[bucket].pairs[hashtable[bucket].last]->key = strdup(key);
    hashtable[bucket].pairs[hashtable[bucket].last]->value = strdup(value);
    hashtable[bucket].pairs = realloc(hashtable[bucket].pairs,
        (++hashtable[bucket].pairssize) * sizeof(struct pair *));
    pthread_mutex_unlock(&hashtable[bucket].array_lock);
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

unsigned long MR_SortedPartition(char *key, int num_partitions) {
    int n;
    unsigned long ul = strtoul(key, NULL, 10);
    for (n = 31; n >= 0; n --)
        if (num_partitions >> n == 1)
            break;
    return (unsigned long) ul >> (32 - n);
}
