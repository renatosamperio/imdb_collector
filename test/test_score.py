from hs_utils import similarity
c = similarity.Similarity()
s1 = 'Ice Age 4 Continental Drift'
s2 = 'Ice Age: Continental Drift'
score = c.score(s1, s2)
# assert(score == 1.0)
print s1
print s2
print score
print "-"*20

s1 = 'Madagascar 3 Europes Most Wanted'
s2 = "Madagascar 3: Europe's Most Wanted"
score = c.score(s1, s2)
assert(score == 1.0)
print s1
print s2
print score
print "-"*20

s1 = 'The Five Year Engagement'
s2 = 'The Five-Year Engagement'
score = c.score(s1, s2)
assert(score == 1.0)
print s1
print s2
print score
print "-"*20

s1 = 'The Odd Life Of Timothy'
s2 = 'The Odd Life of Timothy Green'
score = c.score(s1, s2)
#assert(score == 0.638257265237)
print s1
print s2
print score
print "-"*20

s1 = 'Step Up Revolution Step Up 4 Miami Heat'
s2 = 'Step Up Revolution'
score = c.score(s1, s2)
#assert(score == 0.570882278275)
print s1
print s2
print score
print "-"*20

s1 = 'Men In Black 3 III'
s2 = 'Men in Black 3'
score = c.score(s1, s2)
#assert(score == 0.638257265237)
print s1
print s2
print score
print "-"*20

s1 = 'Madeas Witness Protection'
s2 = "Madea's Witness Protection"
score = c.score(s1, s2)
assert(score == 1.0)
print s1
print s2
print score
print "-"*20

s1 = 'This.is.a.test'
s2 = "This-is-a-test"
score = c.score(s1, s2)
assert(score == 1.0)
print s1
print s2
print score
print "-"*20

s2 = 'This.is.a.test'
s1 = "This-is-a-test"
score = c.score(s1, s2)
assert(score == 1.0)
print s1
print s2
print score
print "-"*20

s2 = 'The Amazing Spider-Man'
s1 = "The Amazing Spiderman"
score = c.score(s1, s2)
#print "cosine1: ",c.cosine_sim(s1, s2)
#print "cosine2: ",c.cosine_sim("The Amazing Spiderman", "The Amazing Spider Man")
#print "cosine3: ",c.cosine_sim("The Amazing Spiderman".lower(), "The Amazing Spider Man".lower())
#assert(score == 0.130277835528)
print s1
print s2
print score
print "-"*20

s2 = 'The Call'
s1 = "The Call Up"
print s1
print s2
score = c.score(s1, s2)
print score
print "cosine1: ",c.cosine_sim(s1, s2)
print "-"*20

## Seven Psychopaths 2012 BDRip Xvid ENG
