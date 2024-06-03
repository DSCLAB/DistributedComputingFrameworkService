#/dcfs-job/dcfsJob watch file:///dcfs-share/dcfs-tmp-json python3 /dcfs-share/dcfs-run/demo.py dcfsInput --configpath /dcfs-job/dcfs-config.xml
#/dcfs-job/dcfsJob watch hds:///join/json python3 /dcfs-share/dcfs-run/demo.py dcfsInput --configpath /dcfs-job/dcfs-config.xml --no-autoresizing --no-autoscaling
/dcfs-job/dcfsJob watch hds:///join/json python3 /dcfs-share/dcfs-run/main.py dcfsInput --configpath /dcfs-job/dcfs-config.xml
