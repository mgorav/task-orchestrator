from metaflow import Flow, get_metadata

print("Current metadata provider: %s" % get_metadata())

for run in Flow('HelloFlow').runs():
    if run.successful:
        print("Hello finished at %s" % run.finished_at)
        print(run.successful)
        # print("Playlist for movies in genre '%s'" % run.data.genre)
        # if run.data.playlist:
        #     print("Top Pick: '%s'" % run.data.playlist[0])
        print('\n')
