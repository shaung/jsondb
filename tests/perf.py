import jsondb

def test():
    db = jsondb.create([])
    for x in xrange(100000):
        db.append(x)

def test2():
    db = []
    for x in xrange(100000):
        db.append(x)

def run(fname):
    import cProfile, pstats
    prof = cProfile.Profile()
    prof.run('%s()' % fname)
    stats = pstats.Stats(prof)
    stats.strip_dirs()
    stats.sort_stats('time', 'calls')
    stats.print_stats(25)
    stats.print_callees()
    #stats.print_callers()


if __name__ == '__main__':
    run('test')
    run('test2')
