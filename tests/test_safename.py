"""Tests for SafeName encoding/decoding and field-boundary escaping."""

from c4py.safename import escape_field, safe_name, unescape_field, unsafe_name


class TestSafeName:
    """SafeName must match Go reference implementation encoding."""

    def test_plain_ascii_passthrough(self):
        """Printable ASCII passes through unchanged."""
        assert safe_name("hello.txt") == "hello.txt"

    def test_unicode_passthrough(self):
        """Printable Unicode passes through unchanged."""
        assert safe_name("cafe\u0301.txt") == "cafe\u0301.txt"  # e + combining acute
        assert safe_name("\u4e16\u754c.txt") == "\u4e16\u754c.txt"  # Chinese chars

    def test_backslash_escape(self):
        """Backslash is Tier 2 escaped."""
        assert safe_name("file\\test.txt") == "file\\\\test.txt"

    def test_null_escape(self):
        """Null byte is Tier 2 escaped as \\0."""
        assert safe_name("file\x00test.txt") == "file\\0test.txt"

    def test_tab_escape(self):
        """Tab is Tier 2 escaped as \\t."""
        assert safe_name("file\ttest.txt") == "file\\ttest.txt"

    def test_newline_escape(self):
        """Newline is Tier 2 escaped as \\n."""
        assert safe_name("file\ntest.txt") == "file\\ntest.txt"

    def test_cr_escape(self):
        """Carriage return is Tier 2 escaped as \\r."""
        assert safe_name("file\rtest.txt") == "file\\rtest.txt"

    def test_currency_sign_tier3(self):
        """Currency sign (U+00A4) is encoded via Tier 3."""
        result = safe_name("\u00a4")
        # ¤ is U+00A4, UTF-8 bytes: 0xC2 0xA4
        # Tier 3: ¤ + braille(0xC2) + braille(0xA4) + ¤
        assert result == "\u00a4\u28c2\u28a4\u00a4"

    def test_control_char_tier3(self):
        """Non-Tier-2 control characters go through Tier 3."""
        # BEL (0x07) is not a Tier 2 character
        result = safe_name("\x07")
        assert result == "\u00a4\u2807\u00a4"

    def test_empty_string(self):
        """Empty string passes through."""
        assert safe_name("") == ""

    def test_all_printable(self):
        """All printable ASCII passthrough."""
        name = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789.-_"
        assert safe_name(name) == name


class TestUnsafeName:
    """UnsafeName must reverse SafeName encoding."""

    def test_plain_passthrough(self):
        assert unsafe_name("hello.txt") == "hello.txt"

    def test_backslash_unescape(self):
        assert unsafe_name("file\\\\test.txt") == "file\\test.txt"

    def test_null_unescape(self):
        assert unsafe_name("file\\0test.txt") == "file\x00test.txt"

    def test_tab_unescape(self):
        assert unsafe_name("file\\ttest.txt") == "file\ttest.txt"

    def test_newline_unescape(self):
        assert unsafe_name("file\\ntest.txt") == "file\ntest.txt"

    def test_cr_unescape(self):
        assert unsafe_name("file\\rtest.txt") == "file\rtest.txt"

    def test_braille_unescape(self):
        """Tier 3 braille range decoded to raw bytes."""
        # ¤ + braille(0x07) + ¤ -> 0x07
        encoded = "\u00a4\u2807\u00a4"
        assert unsafe_name(encoded) == "\x07"

    def test_empty(self):
        assert unsafe_name("") == ""

    def test_lone_backslash(self):
        """Lone backslash or unknown escape passes through."""
        assert unsafe_name("file\\x") == "file\\x"

    def test_lone_currency(self):
        """Lone currency sign without braille passes through."""
        assert unsafe_name("\u00a4abc") == "\u00a4abc"


class TestRoundTrip:
    """Encode then decode must return original."""

    def test_plain(self):
        name = "hello.txt"
        assert unsafe_name(safe_name(name)) == name

    def test_with_backslash(self):
        name = "path\\to\\file.txt"
        assert unsafe_name(safe_name(name)) == name

    def test_with_control_chars(self):
        name = "file\x00\t\n\r.txt"
        assert unsafe_name(safe_name(name)) == name

    def test_with_currency(self):
        name = "\u00a4price.txt"
        assert unsafe_name(safe_name(name)) == name

    def test_unicode(self):
        name = "\u4e16\u754c-world.txt"
        assert unsafe_name(safe_name(name)) == name

    def test_empty(self):
        assert unsafe_name(safe_name("")) == ""

    def test_all_control_chars(self):
        """All byte values 0x00-0x1F round-trip correctly."""
        name = "".join(chr(i) for i in range(32))
        assert unsafe_name(safe_name(name)) == name

    def test_mixed_tiers(self):
        """Mix of Tier 1, 2, and 3 characters."""
        name = "abc\t\x07def"
        assert unsafe_name(safe_name(name)) == name


class TestEscapeField:
    """Field-boundary escaping for c4m output."""

    def test_plain_passthrough(self):
        assert escape_field("hello.txt") == "hello.txt"

    def test_space_escaped(self):
        assert escape_field("my file.txt") == "my\\ file.txt"

    def test_quote_escaped(self):
        assert escape_field('file"test".txt') == 'file\\"test\\".txt'

    def test_brackets_escaped(self):
        assert escape_field("file[1].txt") == "file\\[1\\].txt"

    def test_brackets_not_escaped_for_sequence(self):
        assert escape_field("file[1].txt", is_sequence=True) == "file[1].txt"

    def test_space_and_brackets(self):
        assert escape_field("my file[1].txt") == "my\\ file\\[1\\].txt"


class TestUnescapeField:
    """Reverse field-boundary escaping."""

    def test_plain_passthrough(self):
        assert unescape_field("hello.txt") == "hello.txt"

    def test_space_unescaped(self):
        assert unescape_field("my\\ file.txt") == "my file.txt"

    def test_quote_unescaped(self):
        assert unescape_field('file\\"test\\".txt') == 'file"test".txt'

    def test_brackets_unescaped(self):
        assert unescape_field("file\\[1\\].txt") == "file[1].txt"

    def test_roundtrip(self):
        name = 'my "special" file[1].txt'
        assert unescape_field(escape_field(name)) == name
