from plugins.postgres_kb import PostgresVectorKnowledgeBase


def test_similar_chinese_root_causes_share_canonical_pattern_key():
    kb = PostgresVectorKnowledgeBase.__new__(PostgresVectorKnowledgeBase)

    left = kb._canonicalize_root_cause("系统磁盘空间被日志打满导致服务异常")
    right = kb._canonicalize_root_cause("服务异常的根因是日志占满磁盘空间")

    assert left == "日志占满磁盘"
    assert right == "日志占满磁盘"
    assert kb._build_pattern_key(left) == kb._build_pattern_key(right)


def test_local_tokenizer_keeps_chinese_signal():
    tokens = PostgresVectorKnowledgeBase._tokenize("日志占满磁盘空间")

    assert "日志" in tokens
    assert "磁盘" in tokens
    assert "空间" in tokens
