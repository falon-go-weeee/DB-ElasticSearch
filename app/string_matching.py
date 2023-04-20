import re

def tokenize_address(address):
    tokens = set(re.findall(r'\w+', address.upper()))
    # print("tokens : ", tokens)
    return tokens

def jaccard_similarity(str1, str2):
    set1 = tokenize_address(str1)
    set2 = tokenize_address(str2)

    intersection = set1.intersection(set2)
    # print("intersection : ", intersection)
    union = set1.union(set2)
    # print("union : ", union)
    return len(intersection) / len(union)

def partial_match(match_string, string, threshold):
    matches = []
    substr1 = match_string
    substr2 = string

    match_score = jaccard_similarity(substr1, substr2)
    # print(substr1, " : ", substr2, " : ", match_score)
    if (match_score >= threshold):
        matches.append({"Address":substr2,"Score": match_score})
    # print(match)
    return matches

if __name__ == "__main__":
    # data = partial_match("950 MASON ST", "950 MASON ST, SAN FRANCISCO, CA, 94107", 0.3)
    bno = re.findall('^\d+', "950 MASON ST")
    print(bno)