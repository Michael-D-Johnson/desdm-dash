import os

cfg = open('settings.cfg','r')
lines = cfg.readlines()

shfile = os.path.join(os.environ['HOME'],'.desdm_dash_settings.sh')
with open(shfile,'w') as sh:
    for v in lines:
        sh.write('export {line}'.format(line=v))
