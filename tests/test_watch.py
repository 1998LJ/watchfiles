import sys
import threading
from contextlib import aclosing, contextmanager
from pathlib import Path
from time import sleep
from typing import TYPE_CHECKING

import anyio
import pytest

from watchfiles import Change, awatch, watch
from watchfiles.main import _calc_async_timeout

if TYPE_CHECKING:
    from conftest import MockRustType

try:
    from exceptiongroup import BaseExceptionGroup
except ImportError:
    # outherwise BaseExceptionGroup should be in builtins
    pass


def test_watch(tmp_path: Path, write_soon):
    sleep(0.05)
    write_soon(tmp_path / 'foo.txt')
    changes = None
    for changes in watch(tmp_path, debounce=50, step=10, watch_filter=None):
        break

    assert changes == {(Change.added, str(tmp_path / 'foo.txt'))}


def test_wait_stop_event(tmp_path: Path, write_soon):
    sleep(0.05)
    write_soon(tmp_path / 'foo.txt')

    stop_event = threading.Event()
    for changes in watch(tmp_path, debounce=50, step=10, watch_filter=None, stop_event=stop_event):
        assert changes == {(Change.added, str(tmp_path / 'foo.txt'))}
        stop_event.set()


async def test_awatch(tmp_path: Path, write_soon):
    await anyio.sleep(0.05)
    write_soon(tmp_path / 'foo.txt')
    async for changes in awatch(tmp_path, debounce=50, step=10, watch_filter=None):
        assert changes == {(Change.added, str(tmp_path / 'foo.txt'))}
        break


@pytest.mark.filterwarnings('ignore::DeprecationWarning')
async def test_await_stop_event(tmp_path: Path, write_soon):
    await anyio.sleep(0.05)
    write_soon(tmp_path / 'foo.txt')
    stop_event = anyio.Event()
    async for changes in awatch(tmp_path, debounce=50, step=10, watch_filter=None, stop_event=stop_event):
        assert changes == {(Change.added, str(tmp_path / 'foo.txt'))}
        stop_event.set()


def test_watch_raise_interrupt(mock_rust_notify: 'MockRustType'):
    mock_rust_notify([{(1, 'foo.txt')}], exit_code='signal')

    w = watch('.', raise_interrupt=True)
    assert next(w) == {(Change.added, 'foo.txt')}
    with pytest.raises(KeyboardInterrupt):
        next(w)


def test_watch_dont_raise_interrupt(mock_rust_notify: 'MockRustType', caplog):
    caplog.set_level('WARNING', 'watchfiles')
    mock_rust_notify([{(1, 'foo.txt')}], exit_code='signal')

    w = watch('.', raise_interrupt=False)
    assert next(w) == {(Change.added, 'foo.txt')}
    with pytest.raises(StopIteration):
        next(w)

    assert caplog.text == 'watchfiles.main WARNING: KeyboardInterrupt caught, stopping watch\n'


@contextmanager
def mock_open_signal_receiver(signal):
    async def signals():
        yield signal

    yield signals()


async def test_awatch_unexpected_signal(mock_rust_notify: 'MockRustType'):
    mock_rust_notify([{(1, 'foo.txt')}], exit_code='signal')

    count = 0
    with pytest.raises(RuntimeError, match='watch thread unexpectedly received a signal'):
        async for _ in awatch('.'):
            count += 1

    assert count == 1


async def test_awatch_interrupt_warning(mock_rust_notify: 'MockRustType', caplog):
    mock_rust_notify([{(1, 'foo.txt')}])

    with pytest.warns(DeprecationWarning, match='raise_interrupt is deprecated, KeyboardInterrupt will cause this'):
        changes = [c async for c in awatch('.', raise_interrupt=False)]

    assert len(changes) == 1


def test_watch_no_yield(mock_rust_notify: 'MockRustType', caplog):
    mock = mock_rust_notify([{(1, 'spam.pyc')}, {(1, 'spam.py'), (2, 'ham.txt')}])

    caplog.set_level('INFO', 'watchfiles')
    assert next(watch('.')) == {(Change.added, 'spam.py'), (Change.modified, 'ham.txt')}
    assert mock.watch_count == 2
    assert caplog.text == 'watchfiles.main INFO: 2 changes detected\n'


async def test_awatch_no_yield(mock_rust_notify: 'MockRustType', caplog):
    mock = mock_rust_notify([{(1, 'spam.pyc')}, {(1, 'spam.py')}])

    caplog.set_level('DEBUG', 'watchfiles')
    changes = None
    async for changes in awatch('.'):
        pass

    assert changes == {(Change.added, 'spam.py')}
    assert mock.watch_count == 2
    assert caplog.text == (
        "watchfiles.main DEBUG: all changes filtered out, raw_changes={(1, 'spam.pyc')}\n"
        "watchfiles.main DEBUG: 1 change detected: {(<Change.added: 1>, 'spam.py')}\n"
    )


def test_watch_timeout(mock_rust_notify: 'MockRustType', caplog):
    mock = mock_rust_notify(['timeout', {(1, 'spam.py')}])

    caplog.set_level('DEBUG', 'watchfiles')
    change_list = list(watch('.'))

    assert change_list == [{(Change.added, 'spam.py')}]
    assert mock.watch_count == 2
    assert caplog.text == (
        'watchfiles.main DEBUG: rust notify timeout, continuing\n'
        "watchfiles.main DEBUG: 1 change detected: {(<Change.added: 1>, 'spam.py')}\n"
    )


def test_watch_yield_on_timeout(mock_rust_notify: 'MockRustType'):
    mock = mock_rust_notify(['timeout', {(1, 'spam.py')}])

    change_list = list(watch('.', yield_on_timeout=True))

    assert change_list == [set(), {(Change.added, 'spam.py')}]
    assert mock.watch_count == 2


async def test_awatch_timeout(mock_rust_notify: 'MockRustType', caplog):
    mock = mock_rust_notify(['timeout', {(1, 'spam.py')}])

    caplog.set_level('DEBUG', 'watchfiles')
    change_list = []
    async for changes in awatch('.'):
        change_list.append(changes)

    assert change_list == [{(Change.added, 'spam.py')}]
    assert mock.watch_count == 2
    assert caplog.text == (
        'watchfiles.main DEBUG: rust notify timeout, continuing\n'
        "watchfiles.main DEBUG: 1 change detected: {(<Change.added: 1>, 'spam.py')}\n"
    )


async def test_awatch_yield_on_timeout(mock_rust_notify: 'MockRustType'):
    mock = mock_rust_notify(['timeout', {(1, 'spam.py')}])

    change_list = []
    async for changes in awatch('.', yield_on_timeout=True):
        change_list.append(changes)

    assert change_list == [set(), {(Change.added, 'spam.py')}]
    assert mock.watch_count == 2


@pytest.mark.skipif(sys.platform == 'win32', reason='different on windows')
def test_calc_async_timeout_posix():
    assert _calc_async_timeout(123) == 123
    assert _calc_async_timeout(None) == 5_000


@pytest.mark.skipif(sys.platform != 'win32', reason='different on windows')
def test_calc_async_timeout_win():
    assert _calc_async_timeout(123) == 123
    assert _calc_async_timeout(None) == 1_000


class MockRustNotifyRaise:
    def __init__(self):
        self.i = 0

    def watch(self, *args):
        if self.i == 1:
            raise KeyboardInterrupt('test error')
        self.i += 1
        return {(Change.added, 'spam.py')}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        pass


async def test_awatch_interrupt_raise(mocker):
    mocker.patch('watchfiles.main.RustNotify', return_value=MockRustNotifyRaise())

    count = 0
    stop_event = threading.Event()
    with pytest.raises(BaseExceptionGroup) as exc_info:
        async for _ in awatch('.', stop_event=stop_event):
            count += 1

    assert len(exc_info.value.exceptions) == 1
    exc = exc_info.value.exceptions[0]
    assert isinstance(exc, KeyboardInterrupt)
    assert exc.args == ('test error',)

    # event is set because it's set while handling the KeyboardInterrupt
    assert stop_event.is_set()
    assert count == 1


def test_allow_missing_filters_parent_changes(tmp_path: Path, mock_rust_notify: 'MockRustType'):
    target = tmp_path / 'subdir' / 'new.py'
    unrelated = tmp_path / 'unrelated.py'
    mock_rust_notify([{(1, str(unrelated)), (1, str(target))}])
    watcher = watch(target, allow_missing=True, watch_filter=None)
    try:
        assert next(watcher) == {(Change.added, str(target))}
    finally:
        watcher.close()
    # The real parent (not the nonexistent target) is watched.
    from watchfiles.main import _resolve_watch_paths

    watch_paths, requested_filter = _resolve_watch_paths((target,), None, True)
    assert watch_paths == [str(tmp_path)]
    assert requested_filter is not None
    assert not requested_filter(Change.added, str(unrelated))


async def test_awatch_allow_missing_filters_parent_changes(tmp_path: Path, mock_rust_notify: 'MockRustType'):
    target = tmp_path / 'nested' / 'new.py'
    unrelated = tmp_path / 'other.py'
    mock_rust_notify([{(1, str(unrelated)), (1, str(target))}])
    async for changes in awatch(target, allow_missing=True, watch_filter=None):
        assert changes == {(Change.added, str(target))}
        break


def _signal_watcher_started(monkeypatch: pytest.MonkeyPatch) -> threading.Event:
    """Coordinate writes with actual OS watcher registration, avoiding sleep races."""
    import watchfiles.main as watch_main

    ready = threading.Event()
    real_watcher = watch_main.RustNotify

    def start(*args):
        watcher = real_watcher(*args)
        ready.set()
        return watcher

    monkeypatch.setattr(watch_main, 'RustNotify', start)
    return ready


def test_missing_file_real_events(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    ready = _signal_watcher_started(monkeypatch)
    target = tmp_path / 'new.txt'
    sibling = tmp_path / 'other.txt'

    def write_files():
        if not ready.wait(3):
            return
        sibling.write_text('unrelated')
        target.write_text('tracked')

    thread = threading.Thread(target=write_files)
    thread.start()
    watcher = watch(target, allow_missing=True, debounce=50, step=10, watch_filter=None)
    try:
        assert next(watcher) == {(Change.added, str(target))}
    finally:
        watcher.close()
        thread.join()


def test_missing_file_real_events_polling(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    ready = _signal_watcher_started(monkeypatch)
    target = tmp_path / 'new.txt'

    def write_file():
        if not ready.wait(3):
            return
        sleep(0.1)  # Allow the polling watcher to take its initial snapshot.
        target.write_text('tracked')

    thread = threading.Thread(target=write_file)
    thread.start()
    watcher = watch(
        target, allow_missing=True, debounce=50, step=10, watch_filter=None, force_polling=True, poll_delay_ms=20
    )
    try:
        assert (Change.added, str(target)) in next(watcher)
    finally:
        watcher.close()
        thread.join()


def test_missing_directory_real_events(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    ready = _signal_watcher_started(monkeypatch)
    target_dir = tmp_path / 'created'
    target_file = target_dir / 'new.py'

    def create_directory():
        if not ready.wait(3):
            return
        target_dir.mkdir()
        # Let inotify register the new subdirectory before writing under it.
        sleep(0.1)
        target_file.write_text('tracked')

    thread = threading.Thread(target=create_directory)
    thread.start()
    watcher = watch(target_dir, allow_missing=True, debounce=50, step=10, watch_filter=None)
    try:
        changes = next(watcher)
        assert (Change.added, str(target_dir)) in changes
        assert all(Path(path).is_relative_to(target_dir) for _, path in changes)
    finally:
        watcher.close()
        thread.join()


def test_allow_missing_resolves_nearest_existing_ancestor(tmp_path: Path):
    from watchfiles.main import _resolve_watch_paths

    target = tmp_path / 'no' / 'such' / 'directory'
    sibling = tmp_path / 'no' / 'other'
    paths, path_filter = _resolve_watch_paths((target,), None, True)
    assert paths == [str(tmp_path)]
    assert path_filter is not None
    assert path_filter(Change.added, str(target))
    assert path_filter(Change.added, str(target / 'nested' / 'file.txt'))
    assert not path_filter(Change.added, str(sibling))


async def test_awatch_missing_file_real_events(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    ready = _signal_watcher_started(monkeypatch)
    target = tmp_path / 'later.py'

    async def create_file():
        if await anyio.to_thread.run_sync(ready.wait, 3):
            target.write_text('tracked')

    async with anyio.create_task_group() as tg:
        tg.start_soon(create_file)
        async with aclosing(awatch(target, allow_missing=True, debounce=50, step=10, watch_filter=None)) as watcher:
            async for changes in watcher:
                assert (Change.added, str(target)) in changes
                break


def test_allow_missing_deduplicates_parents(tmp_path: Path):
    from watchfiles.main import _resolve_watch_paths

    paths, event_filter = _resolve_watch_paths((str(tmp_path), tmp_path / 'first', tmp_path / 'second'), None, True)
    assert paths == [str(tmp_path)]
    assert event_filter is not None
    assert event_filter(Change.added, str(tmp_path / 'first'))
    assert event_filter(Change.added, str(tmp_path / 'second'))


def test_allow_missing_respects_user_filter(tmp_path: Path, mock_rust_notify: 'MockRustType'):
    target = tmp_path / 'later.py'
    raw = {(1, str(target)), (2, str(target))}
    mock_rust_notify([raw])
    watcher = watch(
        target,
        allow_missing=True,
        watch_filter=lambda change, path: change is Change.modified,
    )
    try:
        assert next(watcher) == {(Change.modified, str(target))}
    finally:
        watcher.close()


def test_allow_missing_preserves_existing_path_events(tmp_path: Path, mock_rust_notify: 'MockRustType'):
    existing = tmp_path / 'current'
    existing.mkdir()
    missing = tmp_path / 'new.txt'
    existing_file = existing / 'old.txt'
    unrelated = tmp_path / 'stray.txt'
    mock_rust_notify([{(1, str(existing_file)), (1, str(missing)), (1, str(unrelated))}])
    watcher = watch(existing, missing, allow_missing=True, watch_filter=None)
    try:
        assert next(watcher) == {(Change.added, str(existing_file)), (Change.added, str(missing))}
    finally:
        watcher.close()


def test_allow_missing_opt_in_only(tmp_path: Path):
    missing = tmp_path / 'missing.txt'
    watcher = watch(missing)
    try:
        with pytest.raises(FileNotFoundError):
            next(watcher)
    finally:
        watcher.close()
