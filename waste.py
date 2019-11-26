start = timeit.default_timer()
df_append = []
for dat in df_distinct.take(10): #.rdd.collect():
    result = cal_distance_each(dat)
    df_append.append(result)
print('Append Time: ', timeit.default_timer() - start)

df_final = reduce(DataFrame.union, df_append)

print('Calculating Time: ', timeit.default_timer() - start)
