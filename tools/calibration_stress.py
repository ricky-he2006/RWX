"""Make a 30-patch sensitivity batch, including independent heavy SQL fixture edits."""
import json
import random
import runpy
import shutil
import sys
from pathlib import Path

base=Path(sys.argv[1])
out=Path(sys.argv[2])
(out/'commits').mkdir(parents=True,exist_ok=True)
rng=random.Random(2718)
sample=Path('sample')
for patch in rng.sample(sorted((sample/'commits').glob('*.patch')),18):
    shutil.copy(patch,out/'commits')
shutil.copy(sample/'tests.json',out)
# Ten preselected random mutations, five per seed, independent of selector outputs.
for seed in (1301,1302):
    tmp=out/str(seed)
    sys.argv=['genmutants',str(base),str(tmp),'5',str(seed)]
    generators=runpy.run_path('tools/genmutants.py')
    for patch in tmp.glob('*.patch'):
        shutil.copy(patch,out/'commits')
for family in ['h','ds']:
    path=base/f'tests/fixtures/optimizer/tpc-{family}/tpc-{family}.sql'
    new=generators['mutate_fixture'](path)
    patch=generators['unified'](str(path.relative_to(base)),path.read_text(),new)
    (out/'commits'/f'stress_{family}.patch').write_text(patch)
