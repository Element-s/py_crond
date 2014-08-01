#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2014-7-25

@author: Elem
'''
import os
import subprocess
import datetime
import calendar
import threading
import time

MONTH_NAMES = 'jan feb mar apr may jun jul aug sep oct nov dec'.split()
DOW_NAMES = 'sun mon tue wed thu fri sat sun'.split()

FIELDS = MINUTE, HOUR, DOM, MONTH, DOW = range(5)

MINUTE_RANGE = 0, 59, None
HOUR_RANGE = 0, 23, None
DOM_RANGE = 1, 31, None # day of month
MONTH_RANGE = 1, 12, MONTH_NAMES
DOW_RANGE = 0, 7, DOW_NAMES # day of week, beginning sunday

FIELDS_RANGE = MINUTE_RANGE, HOUR_RANGE, DOM_RANGE, MONTH_RANGE, DOW_RANGE

NICK_NAMES = {
    "@yearly" : "0 0 1 1 *",
    "@monthly" : "0 0 1 * *",
    "@weekly" : "0 0 * * 0",
    "@daily" : "0 0 * * *",
    "@hourly" : "0 * * * *"
             }

NICK_NAMES["@annually"] = NICK_NAMES["@yearly"]

class CrondException(Exception):
    """
    Exception raised by crond errors.
    """    
    pass

class Cron(object):

    def __init__(self):
        self.entries = []

    def add(self, *args):
        '''Add a cron job to be executed regularly.'''
        self.entries.append(parse_entry(*args))
    
    def scan_cron(self):
        """
        重新扫描定时器任务，可以通过信号控制新建、删除、编辑任务
        """

    def start(self):
        '''Start the main loop in a new daemon thread.'''
        self.thread = threading.Thread(target=self.main, name='cron.py')
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        '''Stop the daemon and wait for it to exit.'''
        self.stopped = True
        self.thread.join()

    def main(self):
        '''Run jobs when they are supposed to be run, until stopped.'''
        self.stopped = False

        # Run @reboot entries.
        for entry in self.entries:
            if entry.when_reboot:
                self.run_entry(entry)

        while not self.stopped:
            # Sleep until the start of the next minute.
            self.do_sleep()
            now = datetime.datetime.now()

            # Run entries if it is time for them to run.
            for entry in self.entries:
                if entry.should_run(now):
                    self.run_entry(entry)

    def run_entry(self, entry):
        '''Run a cron job in a new thread.'''
        threading.Thread(target=entry, name=entry.name()).start()

    def do_sleep(self):
        '''Sleep until the start of the next minute.'''
        now = int(time.time())
        # Round up to the start of the next minute.
        then = (now + 60) // 60 * 60 
        # Sleep in one second increments so it can exit without
        # a huge delay.
        for i in range(then - now):
            time.sleep(1)
            if self.stopped:
                sys.exit(0)
                

class CronTabEntry(object):

    def __init__(self, entry, fields, command, **flags):
        self.entry = entry
        self.fields = fields
        self.command = command
        self.dom_or_dow_star = flags.get('dom_or_dow_star', False)
        self.when_reboot = flags.get('when_reboot', False)
    
    def name(self):
        """
        Return entry name of command or funciton
        """
        if isinstance(self.command, tuple):
            # Return the name of the python function.
            return self.command[0].__name__
        else:
            return self.command    
    
    def __call__(self):
        if isinstance(self.command, tuple):
            func, args, kwargs = self.command
            return func(*args, **kwargs)
        else:
            with open(os.devnull, 'w') as f_obj:
                if subprocess.call(self.command.split(),
                                   stdout=f_obj,
                                   stderr=f_obj):
                    msg = "command execute return non-zero code!"
                    raise CrondException(msg)
    
    def is_run(self, dt):
        """
        判断该时间点是否执行
        """
        if self.when_reboot:
            return False
        dom = self.get_bit(DOM, dt.day)
        dow = self.get_bit(DOW, dt.weekday())
        return self.get_bit(MINUTE, dt.minute) and \
            self.get_bit(HOUR, dt.hour) and \
            self.get_bit(MONTH, dt.month) and \
            (dom and dow) if (self.dom_or_dow_star) else (dom or dow)
            
    def iter_field(self, field):
        """Iterate through the matching values for a field"""
        low = FIELDS[field][0]
        fields = self.fields[field]
        
        start = 0
        for _ in range(fields.count(True)):
            last_index = fields.index(True, start)
            yield low + last_index
            start = last_index + 1
            
    def next(self):
        """
        迭代下一个可运行时间点
        """
        return next(iter(self))
    
    __next__ = next
            
    def __iter__(self):
        """
        迭代找出定时器条目下一个运行的时间点
        """
        now = datetime.datetime.now()
        year = now.year
        dom_low, dom_high, _ = FIELDS_RANGE[DOM]
        
        while True:
            same_year = (year == now.year)
            for month in self.iter_field(MONTH):                
                if same_year and month < now.month:
                    # 过去的月份不处理
                    continue
                same_month = same_year and (month == now.month)
                num_days = calendar.monthrange(year, month)[1]
                
                # Iterate through all doms, and check later.
                # See Paul Vixie's comment below.
                for dom in range(dom_low, dom_high + 1):
                    if dom > num_days or same_month and dom < now.day:
                        # 大于月的合法天数或者该月逝去的天不处理
                        continue
                    same_day = same_month and dom == now.day
                    
                    # 迭代标记为True的小时
                    for hour in self.iter_field(HOUR):
                        if same_day and hour < now.hour:
                            # 该日逝去的小时，不进行处理
                            continue
                        same_hour = same_day and hour == now.hour
                        for minute in self.iter_field(MINUTE):
                            if same_hour and minute <= now.minute:
                                # 改日逝去的分钟不处理
                                continue
                            
                            # 获取日期是星期几
                            dt = datetime.datetime(
                                    year, month, dom, hour, minute)
                            dow = dt.weekday()
                            
    # From Paul Vixie's cron:
    #/* the dom/dow situation is odd.  '* * 1,15 * Sun' will run on the
    # * first and fifteenth AND every Sunday;  '* * * * Sun' will run *only*
    # * on Sundays;  '* * 1,15 * *' will run *only* the 1st and 15th.  this
    # * is why we keep 'e->dow_star' and 'e->dom_star'.  yes, it's bizarre.
    # * like many bizarre things, it's the standard.
    # */                            
                            valid_dom = dom in list(self.iter_field(DOM)) 
                            valid_dow = dow in list(self.iter_field(DOW))
                            if self.dom_or_dow_star:
                                valid = valid_dom and valid_dow
                            else:
                                valid = valid_dom or valid_dow
                            
                            if valid:
                                yield dt
            
            # 尝试判断下一年的可运行时间点
            year += 1
                            
    
    def get_bit(self, field, bit):
        """
        Return field bit's value
        @param field: field range type
        @param bit: field's bit  
        """ 
        low = FIELDS_RANGE[field][0]
        return self.fields[field][bit - low]
    
                        
def get_command(cmd, func, *args, **kwargs):
    """
    Parse an entry's command.
    @param cmd: entry's command
    @param func: optional entry's func
    @param *args: list args
    @param **kwargs: keyword args    
    """
    cmd = cmd.strip()
    if cmd and func is None:
        return cmd
    elif not cmd and func is not None:
        return func, args, kwargs

    if cmd:
        raise CrondException('found command where none was expected')
    else:
        raise CrondException('expecting a command')

def parse_entry(entry, func, *args, **kwargs):
    """
    Parse tokens of crontab entry
    @param entry: crontab's entry
    @param func: every entry execute function
    @param args: func's args list
    @param kwargs: func's keyword args 
    """
    if func is None and (args or kwargs):
        raise CrondException('arguments were provided, but no command')
    
    entry = entry.strip()
    
    # @reboot time specification: run once, at startup
    if entry.lower().startswith("@reboot"):
        command = get_command(entry[7:], func, args, kwargs)
        return CronTabEntry(entry, None, command, when_reboot=True)
        
    if entry.startswith('@'):
        # other time specifications 'nickname'
        try:
            token = entry.split()[0]
            entry = entry.replace(token, NICK_NAMES[token.lower()], 1)
        except:
            msg = 'bad time specification: {!r}'.format(token)
            raise CrondException(msg)
    
    fields = []
    cmd = str(entry)
    flags = {}
    
    # parse the fields, such as: "0 0 1 1 *"
    for token, field in zip(entry.split(), FIELDS):
        try:
            bits, flags = parse_field(token.lower(), field)
            fields.append(bits)
        except:
            msg = 'error parsing field: {!r}'.format(token)
            raise CrondException(msg)

    command = get_command(cmd, func, args, kwargs)
    if len(fields) < len(FIELDS):
        msg = 'error parsing entry {!r}'.format(entry)
        raise CrondException(msg)        
    
    CronTabEntry(entry, fields, command, flags)

def parse_field(token, field):
    """
    Parse signle token
    @param token: single token, such as: */5
    @param field: field range type 
    @type field
    """
    flags = {}
    
    if not token or not field:
        raise CrondException('token or field is empty!')
    
    # Init every bit is False
    low, high, names = FIELDS_RANGE[field]
    bits = [False for _ in range(high - low + 1)]
    
    # Replace month or week name
    if names is not None:
        for i, name in enumerate(names, low):
            if name in token:
                token = token.replace(name, str(i))
    
    values = token.split(',')
    for val in values:
        step = 1
        if '/' in val:
            val, step = val.split('/')
            try:
                step = int(step)
            except:
                raise CrondException(token)
            if step < 1:
                raise CrondException('Step value must be greater than zero')
            
        if not val:
            raise CrondException(token)
        
        if val == '*':
            # Set the DOM/DOW flag.
            if field in (DOM, DOW):
                flags['dom_or_dow_star'] = True
                
            start, stop = low, high
        elif '-' in val:
            # Set a range of values.
            start, stop = val.split('-')            
            if start.isdigit() and stop.isdigit():
                start, stop = int(start), int(stop)
            else:
                raise CrondException(token)
        elif val.isdigit():
            # Only single digit, set value's bit
            val = int(val)
            if not (low <= val <= high):
                raise CrondException('out of range: {}'.format(val))
            bits[val - low] = True
            continue
        else:
            raise CrondException(token)
        
        if field == DOW and start % 7 == stop % 7:
            start, stop = 0, 7
        if start > stop:
            raise CrondException('start must be less than stop: {!r}'.format(val))
    
        # Set all bits
        for i in range(start, stop + 1, step):
            if not (low <= i <= high):
                raise CrondException('out of range: {}'.format(i))
            bits[i - low] = True
            
    # Both 0 and 7 are Sunday.
    if field == DOW and (bits[0] or bits[7]):
        bits[0] = bits[7] = True       
    
    return bits, flags
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    